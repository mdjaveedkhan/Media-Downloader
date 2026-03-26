import os
import re
import tempfile
import threading
import time
import uuid
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from flask import Flask, Response, jsonify, render_template, request, send_file
from yt_dlp import YoutubeDL


APP_TITLE = "Any Video Downloader"
MAX_FILE_AGE_SECONDS = 60 * 60
MAX_URL_LENGTH = 2048
MAX_COOKIES_TEXT_BYTES = 2 * 1024 * 1024


@dataclass
class DownloadJob:
    id: str
    url: str
    media_type: str
    quality: str
    audio_quality: str
    subtitle_lang: str
    cookies_text: str = ""
    status: str = "queued"
    error: Optional[str] = None
    title: str = ""
    filename: str = ""
    file_path: str = ""
    platform: str = ""
    started_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None
    progress: Dict[str, Any] = field(
        default_factory=lambda: {
            "percent": 0.0,
            "downloaded_bytes": 0,
            "total_bytes": 0,
            "eta_seconds": None,
            "speed_bps": 0,
            "elapsed_seconds": 0,
            "status_text": "Queued",
        }
    )


app = Flask(__name__)
JOBS: Dict[str, DownloadJob] = {}
JOBS_LOCK = threading.Lock()


def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[\\/:*?\"<>|]", "", name).strip()
    return cleaned[:120] if cleaned else "download"


def bytes_to_human(size: float) -> str:
    if size is None or size <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    value = float(size)
    while value >= 1024 and i < len(units) - 1:
        value /= 1024
        i += 1
    return f"{value:.2f} {units[i]}"


def seconds_to_human(total_seconds: Optional[int]) -> str:
    if total_seconds is None or total_seconds < 0:
        return "-"
    mins, sec = divmod(int(total_seconds), 60)
    hrs, mins = divmod(mins, 60)
    if hrs:
        return f"{hrs:02d}:{mins:02d}:{sec:02d}"
    return f"{mins:02d}:{sec:02d}"


def validate_url(url: str) -> str:
    cleaned = (url or "").strip()
    if not cleaned:
        raise ValueError("URL is required.")
    if len(cleaned) > MAX_URL_LENGTH:
        raise ValueError("URL is too long.")

    parsed = urllib.parse.urlparse(cleaned)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Please enter a valid http(s) URL.")

    return cleaned


def validate_cookies_text(cookies_text: str) -> str:
    text = (cookies_text or "").strip()
    if not text:
        return ""

    if len(text.encode("utf-8")) > MAX_COOKIES_TEXT_BYTES:
        raise ValueError("Cookies text is too large.")

    return text


def cleanup_jobs() -> None:
    now = time.time()
    to_delete = []

    with JOBS_LOCK:
        for job_id, job in JOBS.items():
            finished = job.finished_at or job.started_at
            if now - finished <= MAX_FILE_AGE_SECONDS:
                continue

            if job.file_path and os.path.exists(job.file_path):
                try:
                    os.remove(job.file_path)
                except OSError:
                    pass

            base_dir = os.path.join(tempfile.gettempdir(), "any_video_downloader", job.id)
            if os.path.isdir(base_dir):
                try:
                    for name in os.listdir(base_dir):
                        file_to_delete = os.path.join(base_dir, name)
                        if os.path.isfile(file_to_delete):
                            os.remove(file_to_delete)
                    os.rmdir(base_dir)
                except OSError:
                    pass

            to_delete.append(job_id)

        for job_id in to_delete:
            JOBS.pop(job_id, None)


def write_cookies_file(cookies_text: str, target_dir: str) -> Optional[str]:
    cookies_text = (cookies_text or "").strip()
    if not cookies_text:
        return None

    os.makedirs(target_dir, exist_ok=True)
    cookie_file = os.path.join(target_dir, "cookies.txt")
    with open(cookie_file, "w", encoding="utf-8") as handle:
        handle.write(cookies_text)
        if not cookies_text.endswith("\n"):
            handle.write("\n")
    return cookie_file


def pick_thumbnail(info: Dict[str, Any]) -> str:
    thumb = info.get("thumbnail") or ""
    thumbnails = info.get("thumbnails") or []
    best = None
    best_area = -1

    for item in thumbnails:
        if not isinstance(item, dict):
            continue
        url = item.get("url")
        if not url:
            continue
        width = int(item.get("width") or 0)
        height = int(item.get("height") or 0)
        area = width * height
        if area >= best_area:
            best = url
            best_area = area

    return best or thumb


def to_thumbnail_proxy(url: str) -> str:
    if not url:
        return ""
    return "/api/thumbnail?src=" + urllib.parse.quote(url, safe="")


def get_media_info(url: str, cookies_text: str = "") -> Dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="avd_info_") as tmp_dir:
        cookie_file = write_cookies_file(cookies_text, tmp_dir)
        ydl_opts = {
            "quiet": True,
            "skip_download": True,
            "noplaylist": True,
            "nocheckcertificate": True,
            "geo_bypass": True,
        }
        if cookie_file:
            ydl_opts["cookiefile"] = cookie_file

        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)


    if info.get("_type") == "playlist":
        entries = info.get("entries") or []
        if not entries:
            raise ValueError("Playlist has no downloadable entries.")
        info = entries[0]

    formats = info.get("formats") or []

    video_options = []
    seen = set()
    for fmt in formats:
        format_id = str(fmt.get("format_id", "")).strip()
        if not format_id:
            continue

        ext = fmt.get("ext", "")
        vcodec = fmt.get("vcodec")
        if not vcodec or vcodec == "none":
            continue

        height = fmt.get("height") or 0
        fps = fmt.get("fps") or 0
        tbr = fmt.get("tbr") or 0

        label = f"{height}p"
        if fps:
            label += f" {int(fps)}fps"
        if ext:
            label += f" ({ext})"
        if tbr:
            label += f" - {int(tbr)} kbps"

        key = (format_id, label)
        if key in seen:
            continue
        seen.add(key)

        video_options.append(
            {
                "value": format_id,
                "label": label,
                "height": int(height),
                "fps": int(fps),
                "ext": ext,
            }
        )

    video_options.sort(key=lambda x: (x["height"], x["fps"]), reverse=True)

    subtitles = sorted(
        set(list((info.get("subtitles") or {}).keys()) + list((info.get("automatic_captions") or {}).keys()))
    )
    english_subtitles = [lang for lang in subtitles if str(lang).lower().startswith("en")]

    platform = info.get("extractor_key") or info.get("extractor") or "Unknown"
    thumbnail = pick_thumbnail(info)

    return {
        "title": info.get("title") or "Untitled",
        "duration": info.get("duration") or 0,
        "duration_text": seconds_to_human(info.get("duration")),
        "thumbnail": thumbnail,
        "thumbnail_proxy": to_thumbnail_proxy(thumbnail),
        "platform": platform,
        "uploader": info.get("uploader") or "",
        "webpage_url": info.get("webpage_url") or url,
        "video_options": video_options,
        "default_video_quality": "best",
        "audio_options": ["320", "256", "192", "128"],
        "default_audio_quality": "320",
        "subtitles": english_subtitles,
        "english_subtitles_available": bool(english_subtitles),
    }


def find_downloaded_file(job_dir: str) -> Optional[str]:
    if not os.path.isdir(job_dir):
        return None

    candidates = []
    for name in os.listdir(job_dir):
        path = os.path.join(job_dir, name)
        if not os.path.isfile(path):
            continue
        if name.endswith((".part", ".ytdl")):
            continue
        candidates.append(path)

    if not candidates:
        return None
    return max(candidates, key=lambda p: os.path.getsize(p))


def make_progress_hook(job: DownloadJob):
    def hook(d: Dict[str, Any]) -> None:
        with JOBS_LOCK:
            current = JOBS.get(job.id)
            if not current:
                return

            state = d.get("status")
            elapsed = int(time.time() - current.started_at)
            current.progress["elapsed_seconds"] = elapsed

            if state == "downloading":
                downloaded = d.get("downloaded_bytes") or 0
                total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                speed = d.get("speed") or 0
                eta = d.get("eta")
                percent = (downloaded / total * 100) if total else 0.0

                current.progress.update(
                    {
                        "percent": round(percent, 2),
                        "downloaded_bytes": int(downloaded),
                        "total_bytes": int(total),
                        "eta_seconds": int(eta) if eta is not None else None,
                        "speed_bps": int(speed) if speed else 0,
                        "status_text": "Downloading",
                    }
                )
                current.status = "downloading"

            elif state == "finished":
                current.progress.update(
                    {
                        "percent": 100.0,
                        "status_text": "Finalizing",
                    }
                )

    return hook


def run_download(job_id: str) -> None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job:
        return

    try:
        base_dir = os.path.join(tempfile.gettempdir(), "any_video_downloader", job.id)
        os.makedirs(base_dir, exist_ok=True)

        outtmpl = os.path.join(base_dir, "%(title).120s [%(id)s].%(ext)s")

        ydl_opts: Dict[str, Any] = {
            "outtmpl": outtmpl,
            "noplaylist": True,
            "nocheckcertificate": True,
            "geo_bypass": True,
            "quiet": True,
            "no_warnings": True,
            "progress_hooks": [make_progress_hook(job)],
        }
        cookie_file = write_cookies_file(job.cookies_text, base_dir)
        if cookie_file:
            ydl_opts["cookiefile"] = cookie_file

        if job.media_type == "video":
            if job.quality == "best":
                ydl_opts["format"] = "bv*+ba/b"
            else:
                ydl_opts["format"] = f"{job.quality}+ba/b"

            ydl_opts["merge_output_format"] = "mp4"

            # Only request English subtitles/captions when available.
            ydl_opts["writesubtitles"] = True
            ydl_opts["writeautomaticsub"] = True
            ydl_opts["subtitleslangs"] = ["en", "en-US", "en-GB", "en.*"]
            ydl_opts["embedsubtitles"] = True

        else:
            ydl_opts["format"] = "bestaudio/best"
            ydl_opts["postprocessors"] = [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": job.audio_quality,
                }
            ]

        with JOBS_LOCK:
            job.status = "preparing"
            job.progress["status_text"] = "Preparing"

        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(job.url, download=True)
            title = info.get("title") or job.title or "download"

        file_path = find_downloaded_file(base_dir)
        if not file_path:
            raise RuntimeError("Download finished but output file was not found.")

        with JOBS_LOCK:
            current = JOBS.get(job_id)
            if not current:
                return
            current.title = title
            current.file_path = file_path
            current.filename = os.path.basename(file_path)
            current.status = "completed"
            current.finished_at = time.time()
            current.progress.update(
                {
                    "percent": 100.0,
                    "status_text": "Completed",
                    "eta_seconds": 0,
                    "total_bytes": os.path.getsize(file_path),
                    "downloaded_bytes": os.path.getsize(file_path),
                }
            )

    except Exception as exc:
        with JOBS_LOCK:
            failed = JOBS.get(job_id)
            if not failed:
                return
            failed.status = "failed"
            failed.error = str(exc)
            failed.finished_at = time.time()
            failed.progress["status_text"] = "Failed"


@app.route("/")
def index():
    cleanup_jobs()
    return render_template("index.html", app_title=APP_TITLE)


@app.get("/health")
def health():
    return jsonify({"ok": True, "status": "healthy", "service": APP_TITLE})


@app.get("/styles.css")
def styles_proxy():
    styles_path = os.path.join(app.root_path, "templates", "styles.css")
    return send_file(styles_path, mimetype="text/css")


@app.post("/api/info")
def api_info():
    cleanup_jobs()
    data = request.get_json(silent=True) or {}
    try:
        url = validate_url(data.get("url") or "")
        cookies_text = validate_cookies_text(data.get("cookies_text") or "")
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    try:
        info = get_media_info(url, cookies_text=cookies_text)
        return jsonify({"ok": True, "info": info})
    except Exception as exc:
        return jsonify(
            {
                "ok": False,
                "error": (
                    "Could not fetch media info. Some private/age-restricted posts require login cookies "
                    f"and may not be downloadable. Details: {exc}"
                ),
            }
        ), 400


@app.post("/api/download")
def api_download():
    cleanup_jobs()
    data = request.get_json(silent=True) or {}

    try:
        url = validate_url(data.get("url") or "")
        cookies_text = validate_cookies_text(data.get("cookies_text") or "")
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    media_type = (data.get("media_type") or "video").strip().lower()
    quality = (data.get("quality") or "best").strip()
    audio_quality = (data.get("audio_quality") or "320").strip()

    if media_type not in {"video", "mp3"}:
        return jsonify({"ok": False, "error": "Invalid media type."}), 400
    if audio_quality not in {"320", "256", "192", "128"}:
        return jsonify({"ok": False, "error": "Invalid audio quality."}), 400

    job_id = uuid.uuid4().hex
    job = DownloadJob(
        id=job_id,
        url=url,
        media_type=media_type,
        quality=quality,
        audio_quality=audio_quality,
        subtitle_lang="en",
        cookies_text=cookies_text,
    )

    with JOBS_LOCK:
        JOBS[job_id] = job

    thread = threading.Thread(target=run_download, args=(job_id,), daemon=True)
    thread.start()

    return jsonify({"ok": True, "job_id": job_id})


@app.get("/api/progress/<job_id>")
def api_progress(job_id: str):
    cleanup_jobs()
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify({"ok": False, "error": "Job not found."}), 404

        payload = {
            "ok": True,
            "job": {
                "id": job.id,
                "status": job.status,
                "error": job.error,
                "title": job.title,
                "filename": job.filename,
                "progress": {
                    **job.progress,
                    "downloaded_text": bytes_to_human(job.progress.get("downloaded_bytes", 0)),
                    "total_text": bytes_to_human(job.progress.get("total_bytes", 0)),
                    "speed_text": bytes_to_human(job.progress.get("speed_bps", 0)) + "/s",
                    "eta_text": seconds_to_human(job.progress.get("eta_seconds")),
                    "elapsed_text": seconds_to_human(job.progress.get("elapsed_seconds")),
                },
                "download_url": f"/api/file/{job.id}" if job.status == "completed" else "",
            },
        }

    return jsonify(payload)


@app.get("/api/file/<job_id>")
def api_file(job_id: str):
    cleanup_jobs()
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify({"ok": False, "error": "Job not found."}), 404

        if job.status != "completed" or not job.file_path:
            return jsonify({"ok": False, "error": "File is not ready yet."}), 400

        file_path = job.file_path
        suggested_name = sanitize_filename(job.filename or os.path.basename(file_path))

    return send_file(file_path, as_attachment=True, download_name=suggested_name)


@app.get("/api/thumbnail")
def api_thumbnail_proxy():
    src = (request.args.get("src") or "").strip()
    if not src:
        return jsonify({"ok": False, "error": "Missing thumbnail source."}), 400

    parsed = urllib.parse.urlparse(src)
    if parsed.scheme not in {"http", "https"}:
        return jsonify({"ok": False, "error": "Invalid thumbnail source."}), 400

    req = urllib.request.Request(
        src,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            )
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            body = response.read()
            content_type = response.headers.get("Content-Type", "image/jpeg")
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Thumbnail fetch failed: {exc}"}), 400

    return Response(body, mimetype=content_type)


@app.errorhandler(404)
def not_found(_):
    return jsonify({"ok": False, "error": "Endpoint not found."}), 404


@app.errorhandler(500)
def internal_error(_):
    return jsonify({"ok": False, "error": "Internal server error."}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)

