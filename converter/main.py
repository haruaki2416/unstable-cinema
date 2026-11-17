import os, uuid, threading, tempfile, subprocess, secrets
from pathlib import Path
from typing import Dict, Any
import boto3
from botocore.config import Config
from fastapi import FastAPI, HTTPException, Header
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

PASSWORD = os.getenv("APP_PASSWORD", "")
BUCKET = os.environ["BUCKET_NAME"]
ENDPOINT = os.environ["S3_ENDPOINT"]
REGION = os.getenv("S3_REGION", "sin")
ACCESS = os.environ["S3_ACCESS_KEY_ID"]
SECRET = os.environ["S3_SECRET_ACCESS_KEY"]
PART_SIZE = 64 * 1024 * 1024
s3 = boto3.client("s3", endpoint_url=ENDPOINT, region_name=REGION,
    aws_access_key_id=ACCESS, aws_secret_access_key=SECRET,
    config=Config(signature_version="s3v4", s3={"addressing_style": os.getenv("S3_ADDRESSING_STYLE", "path")}))
app = FastAPI(title="Unstable Converter")
jobs: Dict[str, Dict[str, Any]] = {}
uploads: Dict[str, Dict[str, Any]] = {}
lock = threading.Lock()

def check_auth(value: str | None):
    if not PASSWORD:
        raise HTTPException(503, "APP_PASSWORD is not configured.")
    if not value or not secrets.compare_digest(value, PASSWORD):
        raise HTTPException(401, "Incorrect access password.")

class InitUpload(BaseModel):
    filename: str
    size: int
class CompleteUpload(BaseModel):
    upload_id: str
    key: str
    parts: list[dict]
class StartJob(BaseModel):
    key: str
    srt_text: str
    subtitle_language: str = "und"

@app.on_event("startup")
def configure_bucket_cors():
    try:
        s3.put_bucket_cors(Bucket=BUCKET, CORSConfiguration={"CORSRules":[{"AllowedOrigins":["*"],"AllowedMethods":["PUT","GET","HEAD"],"AllowedHeaders":["*"],"ExposeHeaders":["ETag"],"MaxAgeSeconds":3600}]})
    except Exception:
        pass

def run_job(job_id: str, input_key: str, srt_text: str, language: str):
    output_key = f"output/{job_id}.mp4"
    upload_id = None
    try:
        with lock: jobs[job_id].update(status="processing", message="Preparing the video and subtitle track…")
        input_url = s3.generate_presigned_url("get_object", Params={"Bucket": BUCKET, "Key": input_key}, ExpiresIn=43200)
        with tempfile.TemporaryDirectory() as td:
            srt_path = Path(td) / "subtitles.srt"
            srt_path.write_text(srt_text.lstrip("\ufeff"), encoding="utf-8")
            cmd = ["ffmpeg","-hide_banner","-loglevel","error","-i",input_url,"-i",str(srt_path),
                   "-map","0:v?","-map","0:a?","-map","1:0","-c","copy","-c:s","mov_text",
                   "-metadata:s:s:0",f"language={language}","-movflags","+frag_keyframe+empty_moov+default_base_moof","-f","mp4","pipe:1"]
            with tempfile.TemporaryFile() as errfile:
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=errfile, bufsize=0)
                created = s3.create_multipart_upload(Bucket=BUCKET, Key=output_key, ContentType="video/mp4",
                    Metadata={"job-id":job_id})
                upload_id = created["UploadId"]
                parts, part_no, buffer = [], 1, bytearray()
                while True:
                    chunk = proc.stdout.read(8 * 1024 * 1024)
                    if not chunk: break
                    buffer.extend(chunk)
                    while len(buffer) >= 16 * 1024 * 1024:
                        payload = bytes(buffer[:16 * 1024 * 1024]); del buffer[:16 * 1024 * 1024]
                        result = s3.upload_part(Bucket=BUCKET,Key=output_key,UploadId=upload_id,PartNumber=part_no,Body=payload)
                        parts.append({"PartNumber":part_no,"ETag":result["ETag"]}); part_no += 1
                        with lock: jobs[job_id]["message"] = f"Muxing video… output chunk {part_no-1}"
                code = proc.wait()
                errfile.seek(0); error_text = errfile.read().decode("utf-8","replace")
                if code != 0: raise RuntimeError(error_text[-2500:] or f"FFmpeg exited with code {code}")
                if buffer:
                    result = s3.upload_part(Bucket=BUCKET,Key=output_key,UploadId=upload_id,PartNumber=part_no,Body=bytes(buffer))
                    parts.append({"PartNumber":part_no,"ETag":result["ETag"]})
                s3.complete_multipart_upload(Bucket=BUCKET,Key=output_key,UploadId=upload_id,MultipartUpload={"Parts":parts})
                upload_id = None
        try: s3.delete_object(Bucket=BUCKET,Key=input_key)
        except Exception: pass
        with lock: jobs[job_id].update(status="complete",message="Done. The MP4 now contains a selectable subtitle track.",output_key=output_key)
    except Exception as exc:
        if upload_id:
            try: s3.abort_multipart_upload(Bucket=BUCKET,Key=output_key,UploadId=upload_id)
            except Exception: pass
        try: s3.delete_object(Bucket=BUCKET,Key=input_key)
        except Exception: pass
        with lock: jobs[job_id].update(status="failed",message=str(exc)[:1800])

@app.get("/health")
def health(): return {"ok":True}
@app.get("/api/auth")
def auth(authorization: str | None = Header(default=None)):
    check_auth(authorization); return {"ok":True}
@app.post("/api/upload/init")
def init_upload(body: InitUpload, authorization: str | None = Header(default=None)):
    check_auth(authorization)
    if body.size <= 0 or body.size > 30*1024*1024*1024: raise HTTPException(400,"Video must be between 1 byte and 30 GB.")
    key=f"input/{uuid.uuid4()}-{Path(body.filename).name[:100]}"
    result=s3.create_multipart_upload(Bucket=BUCKET,Key=key,ContentType="video/mp4")
    token=str(uuid.uuid4()); uploads[token]={"key":key,"upload_id":result["UploadId"],"size":body.size}
    return {"token":token,"key":key,"upload_id":result["UploadId"],"part_size":PART_SIZE}
@app.post("/api/upload/{token}/part-url")
def part_url(token: str, part_number: int, authorization: str | None = Header(default=None)):
    check_auth(authorization); item=uploads.get(token)
    if not item or not 1<=part_number<=10000: raise HTTPException(404,"Upload session not found.")
    url=s3.generate_presigned_url("upload_part",Params={"Bucket":BUCKET,"Key":item["key"],"UploadId":item["upload_id"],"PartNumber":part_number},ExpiresIn=3600)
    return {"url":url}
@app.post("/api/upload/{token}/complete")
def complete_upload(token: str, body: CompleteUpload, authorization: str | None = Header(default=None)):
    check_auth(authorization); item=uploads.get(token)
    if not item or body.upload_id!=item["upload_id"] or body.key!=item["key"]: raise HTTPException(404,"Upload session not found.")
    try:
        ordered=sorted(body.parts,key=lambda p:p["PartNumber"])
        s3.complete_multipart_upload(Bucket=BUCKET,Key=item["key"],UploadId=item["upload_id"],MultipartUpload={"Parts":ordered})
    except Exception as exc: raise HTTPException(400,f"Could not finish upload: {str(exc)[:400]}")
    uploads.pop(token,None); return {"key":item["key"]}
@app.post("/api/process")
def process(body: StartJob, authorization: str | None = Header(default=None)):
    check_auth(authorization)
    if len(body.srt_text)>15_000_000 or "-->" not in body.srt_text: raise HTTPException(400,"That does not look like a valid SRT subtitle file.")
    try: s3.head_object(Bucket=BUCKET,Key=body.key)
    except Exception: raise HTTPException(404,"Uploaded video was not found.")
    job_id=str(uuid.uuid4())
    with lock: jobs[job_id]={"status":"queued","message":"Queued for processing."}
    threading.Thread(target=run_job,args=(job_id,body.key,body.srt_text,body.subtitle_language),daemon=True).start()
    return {"job_id":job_id}
@app.get("/api/jobs/{job_id}")
def job_status(job_id: str, authorization: str | None = Header(default=None)):
    check_auth(authorization)
    with lock:
        job=jobs.get(job_id)
        if not job: raise HTTPException(404,"Job not found.")
        result=dict(job)
    if result.get("status")=="complete":
        result["download_url"]=s3.generate_presigned_url("get_object",Params={"Bucket":BUCKET,"Key":result["output_key"],"ResponseContentDisposition":'attachment; filename="unstable-converted.mp4"'},ExpiresIn=3600)
    return result
@app.delete("/api/jobs/{job_id}")
def delete_job(job_id: str, authorization: str | None = Header(default=None)):
    check_auth(authorization)
    with lock: job=jobs.pop(job_id,None)
    if job and job.get("output_key"): s3.delete_object(Bucket=BUCKET,Key=job["output_key"])
    return {"deleted":True}
@app.delete("/api/uploads/{token}")
def cancel_upload(token: str, authorization: str | None = Header(default=None)):
    check_auth(authorization); item=uploads.pop(token,None)
    if item: s3.abort_multipart_upload(Bucket=BUCKET,Key=item["key"],UploadId=item["upload_id"])
    return {"cancelled":True}
app.mount("/",StaticFiles(directory="static",html=True),name="static")
