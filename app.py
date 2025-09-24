import os, ssl
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from ftplib import FTP_TLS, error_perm

import socket
from ftplib import FTP_TLS

class FTPSessionReuse(FTP_TLS):
    """FTP_TLS с повторным использованием TLS-сессии для data-соединений."""
    def ntransfercmd(self, cmd, rest=None):
        # Базовая логика из FTP_TLS, но добавляем session=self.sock.session
        size = None
        if self.passiveserver:
            host, port = self.makepasv()
            conn = socket.create_connection((host, port), self.timeout, self.source_address)
            if self._prot_p:
                conn = self.context.wrap_socket(
                    conn,
                    server_hostname=self.host,
                    session=getattr(self.sock, "session", None)  # <-- ключевая строчка
                )
            return conn, size
        else:
            raise OSError("Active mode is not supported in this client")

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
    # максимально совместимый SSL-контекст
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    try:
        ctx.minimum_version = ssl.TLSVersion.TLSv1
        ctx.maximum_version = ssl.TLSVersion.TLSv1_2
    except Exception:
        pass
    try:
        ctx.set_ciphers("DEFAULT:@SECLEVEL=1")
        ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
        ctx.options |= ssl.OP_IGNORE_UNEXPECTED_EOF
    except Exception:
        pass
    if DISABLE_TLS_VERIFY:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    ftps = FTPSessionReuse(context=ctx, timeout=30)   # <-- используем подкласс
    ftps.connect(HOST, PORT, timeout=30)
    print("[FTPS] auth()…")
    ftps.auth()                       # AUTH TLS
    print("[FTPS] login()…")
    ftps.login(USER, PASSWORD)
    print("[FTPS] prot_p()…")
    ftps.prot_p()                     # защищаем data-канал
    ftps.set_pasv(True)
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
