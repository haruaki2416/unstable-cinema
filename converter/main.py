import os, uuid, threading, tempfile, subprocess, secrets, json, queue
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
    config=Config(signature_version="s3v4", s3={"addressing_style": os.getenv("S3_ADDRESSING_STYLE", "path")}, connect_timeout=20, read_timeout=180, retries={"max_attempts": 3, "mode": "standard"}, tcp_keepalive=True))
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
class RetryLatest(BaseModel):
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
    proc = None
    error_text = ""

    def stage(message: str, **fields):
        with lock:
            if job_id in jobs:
                jobs[job_id].update(status="processing", message=message, **fields)

    try:
        stage("Checking original video in storage…")
        input_size = s3.head_object(Bucket=BUCKET, Key=input_key)["ContentLength"]

        with tempfile.TemporaryDirectory(prefix="unstable-mux-") as td:
            source_path = Path(td) / "source.mp4"
            srt_path = Path(td) / "subtitles.srt"

            stage("Downloading original from storage…", stage="download")
            source_obj = s3.get_object(Bucket=BUCKET, Key=input_key)
            downloaded = 0
            with source_path.open("wb") as source_file:
                body = source_obj["Body"]
                try:
                    while True:
                        block = body.read(16 * 1024 * 1024)
                        if not block:
                            break
                        source_file.write(block)
                        downloaded += len(block)
                        stage(f"Preparing source… {downloaded:,} / {input_size:,} bytes", stage="download")
                finally:
                    body.close()
            if downloaded != input_size:
                raise RuntimeError(f"Source download incomplete: received {downloaded:,} of {input_size:,} bytes. Original upload remains preserved.")

            srt_path.write_text(srt_text.lstrip("\ufeff"), encoding="utf-8")
            cmd = [
                "ffmpeg", "-hide_banner", "-loglevel", "warning",
                "-probesize", "100M", "-analyzeduration", "100M",
                "-i", str(source_path), "-i", str(srt_path),
                "-map", "0:v:0", "-map", "0:a?", "-map", "1:0",
                "-c:v", "copy", "-c:a", "copy", "-c:s", "mov_text",
                "-metadata:s:s:0", f"language={language}",
                "-movflags", "+frag_keyframe+empty_moov+default_base_moof",
                "-f", "mp4", "pipe:1"
            ]

            stage("Starting FFmpeg and multipart output upload…", stage="mux_start")
            with tempfile.TemporaryFile() as errfile:
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=errfile, bufsize=0)
                created = s3.create_multipart_upload(
                    Bucket=BUCKET, Key=output_key, ContentType="video/mp4",
                    Metadata={"job-id": job_id}
                )
                upload_id = created["UploadId"]
                parts, part_no, buffer = [], 1, bytearray()
                output_bytes = 0
                chunks = queue.Queue(maxsize=2)
                end_of_stream = object()

                def read_ffmpeg_stdout():
                    try:
                        while True:
                            data = proc.stdout.read(8 * 1024 * 1024)
                            if not data:
                                break
                            chunks.put(data)
                    except Exception as read_error:
                        chunks.put(read_error)
                    finally:
                        chunks.put(end_of_stream)

                reader = threading.Thread(target=read_ffmpeg_stdout, daemon=True)
                reader.start()

                stage("Muxing video… waiting for FFmpeg output", stage="mux")
                while True:
                    try:
                        item = chunks.get(timeout=600)
                    except queue.Empty:
                        raise RuntimeError("FFmpeg produced no output for 10 minutes. The mux was stopped to avoid an endless job.")
                    if item is end_of_stream:
                        break
                    if isinstance(item, Exception):
                        raise RuntimeError(f"Could not read FFmpeg output: {item}")
                    buffer.extend(item)
                    output_bytes += len(item)
                    while len(buffer) >= 16 * 1024 * 1024:
                        payload = bytes(buffer[:16 * 1024 * 1024])
                        del buffer[:16 * 1024 * 1024]
                        stage(f"Uploading muxed output part {part_no}… ({output_bytes:,} bytes generated)", stage="upload_parts")
                        result = s3.upload_part(
                            Bucket=BUCKET, Key=output_key, UploadId=upload_id,
                            PartNumber=part_no, Body=payload
                        )
                        parts.append({"PartNumber": part_no, "ETag": result["ETag"]})
                        part_no += 1

                stage("FFmpeg finished writing output; checking exit status…", stage="ffmpeg_exit")
                code = proc.wait(timeout=30)
                proc.stdout.close()
                errfile.seek(0)
                error_text = errfile.read().decode("utf-8", "replace")
                if code != 0:
                    raise RuntimeError(error_text[-2500:] or f"FFmpeg exited with code {code}")
                if not parts and not buffer:
                    raise RuntimeError("FFmpeg completed but produced an empty MP4.")

                if buffer:
                    stage(f"Uploading final partial output part {part_no}…", stage="final_part")
                    result = s3.upload_part(
                        Bucket=BUCKET, Key=output_key, UploadId=upload_id,
                        PartNumber=part_no, Body=bytes(buffer)
                    )
                    parts.append({"PartNumber": part_no, "ETag": result["ETag"]})

                stage(f"Finalizing storage upload ({len(parts)} parts)…", stage="complete_multipart")
                s3.complete_multipart_upload(
                    Bucket=BUCKET, Key=output_key, UploadId=upload_id,
                    MultipartUpload={"Parts": parts}
                )
                upload_id = None

            stage("Verifying completed MP4 in storage…", stage="verify_storage")
            output_info = s3.head_object(Bucket=BUCKET, Key=output_key)
            output_size = output_info["ContentLength"]
            if output_size != output_bytes:
                raise RuntimeError(f"Output size verification failed: FFmpeg generated {output_bytes:,} bytes but storage reports {output_size:,} bytes.")
            if output_size < max(5 * 1024 * 1024, int(input_size * 0.80)):
                raise RuntimeError(f"Output validation failed: generated MP4 is only {output_size:,} bytes for a {input_size:,}-byte source. FFmpeg may have stopped reading the source early. FFmpeg diagnostics: {error_text[-3000:] or '(no warning or error text was emitted)'}. Original upload has been preserved.")

            stage("Checking video and selectable subtitle streams…", stage="ffprobe")
            output_url = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": BUCKET, "Key": output_key},
                ExpiresIn=3600
            )
            probe = subprocess.run(
                ["ffprobe", "-v", "error", "-rw_timeout", "30000000",
                 "-probesize", "5000000", "-analyzeduration", "5000000",
                 "-show_entries", "stream=codec_type", "-of", "json", output_url],
                capture_output=True, text=True, timeout=300
            )
            if probe.returncode != 0:
                raise RuntimeError("Output validation failed: ffprobe could not read the generated MP4. Original upload has been preserved. " + probe.stderr[-1200:])
            stream_types = [item.get("codec_type") for item in json.loads(probe.stdout).get("streams", [])]
            if "video" not in stream_types or "subtitle" not in stream_types:
                raise RuntimeError(f"Output validation failed: expected video and subtitle streams, found {stream_types}. Original upload has been preserved.")

        with lock:
            jobs[job_id].update(
                status="complete",
                stage="complete",
                message=f"Validated MP4 ready ({output_size:,} bytes) with video and selectable subtitle streams. Original upload preserved for retries.",
                output_key=output_key,
                output_size=output_size
            )

    except Exception as exc:
        if proc is not None and proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=10)
            except Exception:
                try:
                    proc.kill()
                    proc.wait(timeout=5)
                except Exception:
                    pass
        if upload_id:
            try:
                s3.abort_multipart_upload(Bucket=BUCKET, Key=output_key, UploadId=upload_id)
            except Exception:
                pass
        try:
            s3.delete_object(Bucket=BUCKET, Key=output_key)
        except Exception:
            pass
        with lock:
            if job_id in jobs:
                jobs[job_id].update(
                    status="failed",
                    stage="failed",
                    message=str(exc)[:1800],
                    input_preserved=True
                )

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
    token=str(uuid.uuid4()); item={"key":key,"upload_id":result["UploadId"],"size":body.size,"filename":Path(body.filename).name}
    uploads[token]=item
    s3.put_object(Bucket=BUCKET,Key=f"sessions/{token}.json",Body=json.dumps(item).encode(),ContentType="application/json")
    return {"token":token,"key":key,"upload_id":result["UploadId"],"part_size":PART_SIZE}
SAVED_UPLOAD_KEY = "sessions/first-upload.json"

def remember_first_upload(item: dict, replace: bool = False):
    """Persist the first completed source video so retries never depend on job memory."""
    if not replace:
        try:
            s3.head_object(Bucket=BUCKET, Key=SAVED_UPLOAD_KEY)
            return
        except Exception:
            pass
    saved = {
        "key": item["key"],
        "filename": item.get("filename", Path(item["key"]).name),
        "size": item.get("size"),
    }
    s3.put_object(
        Bucket=BUCKET,
        Key=SAVED_UPLOAD_KEY,
        Body=json.dumps(saved).encode(),
        ContentType="application/json",
    )

def load_upload(token: str):
    try:
        obj=s3.get_object(Bucket=BUCKET,Key=f"sessions/{token}.json")
        return json.loads(obj["Body"].read().decode())
    except Exception:
        item=uploads.get(token)
        if item: return item
        raise HTTPException(404,"Saved upload session not found.")
@app.get("/api/upload/{token}/parts")
def uploaded_parts(token: str, authorization: str | None = Header(default=None)):
    check_auth(authorization); item=load_upload(token)
    try:
        response=s3.list_parts(Bucket=BUCKET,Key=item["key"],UploadId=item["upload_id"])
        parts=[{"PartNumber":p["PartNumber"],"ETag":p["ETag"]} for p in response.get("Parts",[])]
        return {"parts":parts,"key":item["key"],"size":item["size"],"filename":item["filename"]}
    except Exception as exc: raise HTTPException(400,f"Could not inspect saved upload: {str(exc)[:300]}")
@app.post("/api/upload/{token}/part-url")
def part_url(token: str, part_number: int, authorization: str | None = Header(default=None)):
    check_auth(authorization); item=load_upload(token)
    if not 1<=part_number<=10000: raise HTTPException(400,"Invalid chunk number.")
    url=s3.generate_presigned_url("upload_part",Params={"Bucket":BUCKET,"Key":item["key"],"UploadId":item["upload_id"],"PartNumber":part_number},ExpiresIn=3600)
    return {"url":url}
@app.post("/api/upload/{token}/complete")
def complete_upload(token: str, body: CompleteUpload, authorization: str | None = Header(default=None)):
    check_auth(authorization); item=load_upload(token)
    if body.upload_id!=item["upload_id"] or body.key!=item["key"]: raise HTTPException(404,"Upload session mismatch.")
    try:
        listed=s3.list_parts(Bucket=BUCKET,Key=item["key"],UploadId=item["upload_id"])
        actual=sorted([{"PartNumber":p["PartNumber"],"ETag":p["ETag"]} for p in listed.get("Parts",[])],key=lambda p:p["PartNumber"])
        if not actual: raise HTTPException(400,"No uploaded chunks were found.")
        ordered=sorted(body.parts,key=lambda p:p["PartNumber"])
        if ordered != actual: ordered=actual
        s3.complete_multipart_upload(Bucket=BUCKET,Key=item["key"],UploadId=item["upload_id"],MultipartUpload={"Parts":ordered})
        uploaded_size = s3.head_object(Bucket=BUCKET, Key=item["key"])["ContentLength"]
        if uploaded_size != item["size"]:
            s3.delete_object(Bucket=BUCKET, Key=item["key"])
            raise HTTPException(400, f"Upload size mismatch: expected {item['size']:,} bytes but storage contains {uploaded_size:,} bytes.")
    except HTTPException:
        raise
    except Exception as exc: raise HTTPException(400,f"Could not finish upload: {str(exc)[:400]}")
    # Keep the completed source MP4 in input/ and remember the first one
    # permanently for retry. Successful output creation never deletes this object.
    try:
        remember_first_upload(item)
    except Exception:
        pass
    uploads.pop(token,None)
    try: s3.delete_object(Bucket=BUCKET,Key=f"sessions/{token}.json")
    except Exception: pass
    return {"key":item["key"]}
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
@app.delete("/api/saved-source")
def delete_saved_source(authorization: str | None = Header(default=None)):
    check_auth(authorization)
    try:
        saved_obj = s3.get_object(Bucket=BUCKET, Key=SAVED_UPLOAD_KEY)
        saved = json.loads(saved_obj["Body"].read().decode())
        key = saved.get("key")
        if not key or not key.startswith("input/"):
            raise HTTPException(409, "Saved source pointer is invalid; no object was deleted.")
        try:
            head = s3.head_object(Bucket=BUCKET, Key=key)
            size = head.get("ContentLength", 0)
            s3.delete_object(Bucket=BUCKET, Key=key)
        except Exception as exc:
            raise HTTPException(502, f"Could not delete saved source object: {str(exc)[:300]}")
        s3.delete_object(Bucket=BUCKET, Key=SAVED_UPLOAD_KEY)
        return {"deleted": True, "key": key, "size": size}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(404, f"No saved original was found: {str(exc)[:300]}")

@app.post("/api/retry-latest")
def retry_latest(body: RetryLatest, authorization: str | None = Header(default=None)):
    check_auth(authorization)
    if len(body.srt_text)>15_000_000 or "-->" not in body.srt_text:
        raise HTTPException(400,"That does not look like a valid SRT subtitle file.")
    try:
        # Prefer the durable first-upload pointer, so a retry keeps using the
        # original source even after outputs or later uploads are created.
        key = None
        try:
            saved_obj = s3.get_object(Bucket=BUCKET, Key=SAVED_UPLOAD_KEY)
            saved = json.loads(saved_obj["Body"].read().decode())
            candidate_key = saved.get("key")
            if candidate_key:
                s3.head_object(Bucket=BUCKET, Key=candidate_key)
                key = candidate_key
        except Exception:
            key = None
        if not key:
            response=s3.list_objects_v2(Bucket=BUCKET,Prefix="input/")
            candidates=[o for o in response.get("Contents",[]) if o["Key"].lower().endswith(".mp4")]
            if not candidates: raise HTTPException(404,"No saved MP4 upload was found in input/.")
            latest=max(candidates,key=lambda o:o["LastModified"])
            key=latest["Key"]
            s3.head_object(Bucket=BUCKET,Key=key)
            # Recover the durable pointer if it was missing or stale.
            remember_first_upload({"key":key,"filename":Path(key).name}, replace=True)
    except HTTPException: raise
    except Exception as exc: raise HTTPException(500,f"Could not find saved upload: {str(exc)[:300]}")
    job_id=str(uuid.uuid4())
    with lock: jobs[job_id]={"status":"queued","message":f"Retrying saved upload: {Path(key).name}"}
    threading.Thread(target=run_job,args=(job_id,key,body.srt_text,body.subtitle_language),daemon=True).start()
    return {"job_id":job_id,"source":Path(key).name}
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
    check_auth(authorization)
    try:
        item=load_upload(token)
        s3.abort_multipart_upload(Bucket=BUCKET,Key=item["key"],UploadId=item["upload_id"])
        s3.delete_object(Bucket=BUCKET,Key=f"sessions/{token}.json")
        uploads.pop(token,None)
    except HTTPException: pass
    return {"cancelled":True}
app.mount("/",StaticFiles(directory="static",html=True),name="static")
