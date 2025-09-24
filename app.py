import os, ssl
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from ftplib import FTP_TLS, error_perm

N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "").strip()
LOCAL_PATH = Path(os.getenv("LOCAL_PATH", "/data/ostatki.xlsx"))
HOST     = os.getenv("FTPS_HOST", "195.239.40.222")
PORT     = int(os.getenv("FTPS_PORT", "19121"))
USER     = os.getenv("FTPS_USER", "FTP_1C")
PASSWORD = os.getenv("FTPS_PASSWORD", "W8JBV!H2RuzMwc64")
REMOTE_DIR = os.getenv("FTPS_DIR", "/FTP_1C")
MASK_EXT = tuple(e.strip() for e in os.getenv("MASK_EXT", ".xlsx,.ods").split(","))

PASV = os.getenv("PASV", "true").lower() == "true"
DISABLE_TLS_VERIFY = os.getenv("DISABLE_TLS_VERIFY", "true").lower() == "true"

def connect_ftps():
    ctx = ssl.create_default_context()
    if DISABLE_TLS_VERIFY:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    ftps = FTP_TLS(context=ctx)
    ftps.connect(HOST, PORT, timeout=30)
    ftps.auth()
    ftps.login(USER, PASSWORD)
    ftps.prot_p()
    ftps.set_pasv(PASV)
    ftps.encoding = "utf-8"
    return ftps

def list_files(ftps):
    ftps.cwd(REMOTE_DIR)
    files = []
    try:
        for name, facts in ftps.mlsd():
            if facts.get("type") == "file" and name.endswith(MASK_EXT):
                ts = facts.get("modify")
                mtime = datetime.strptime(ts, "%Y%m%d%H%M%S") if ts else None
                size = int(facts.get("size") or 0)
                files.append({"name": name, "mtime": mtime, "size": size})
    except error_perm:
        for name in ftps.nlst():
            if name.endswith(MASK_EXT):
                files.append({"name": name, "mtime": None, "size": None})
    return files

def pick_latest(files):
    return sorted(files,
                  key=lambda x: (x["mtime"] is None, x["mtime"] or datetime.min,
                                 x["size"] or 0, x["name"]),
                  reverse=True)[0]["name"]

def download(ftps, remote_name, local_path: Path):
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("wb", delete=False, dir=str(local_path.parent)) as tmp:
        ftps.retrbinary(f"RETR {remote_name}", tmp.write)
        tmp_path = Path(tmp.name)
    tmp_path.replace(local_path)
    return local_path

def post_to_n8n(local_path: Path):
    if not N8N_WEBHOOK_URL:
        print("[INFO] N8N_WEBHOOK_URL not set; skipping webhook.")
        return
    import requests
    with open(local_path, "rb") as f:
        r = requests.post(N8N_WEBHOOK_URL, files={"file": (local_path.name, f)})
    r.raise_for_status()
    print(f"[OK] Sent to n8n webhook: {r.status_code}")

def run():
    ftps = connect_ftps()
    try:
        files = list_files(ftps)
        if not files: raise RuntimeError("No files on server match mask.")
        latest = pick_latest(files)
        print(f"[INFO] Latest on server: {latest}")
        path = download(ftps, latest, LOCAL_PATH)
        print(f"[OK] Saved to {path}")
    finally:
        try: ftps.quit()
        except Exception: pass
    post_to_n8n(LOCAL_PATH)

if __name__ == "__main__":
    run()
