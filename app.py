import os, ssl, socket
from pathlib import Path
from tempfile import NamedTemporaryFile
from ftplib import FTP_TLS

N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "").strip()
LOCAL_PATH = Path(os.getenv("LOCAL_PATH", "/data/ostatki.xlsx"))

HOST     = os.getenv("FTPS_HOST", "195.239.40.222")
PORT     = int(os.getenv("FTPS_PORT", "19121"))
USER     = os.getenv("FTPS_USER", "FTP_1C")
PASSWORD = os.getenv("FTPS_PASSWORD", "W8JBV!H2RuzMwc64")

REMOTE_DIR  = os.getenv("FTPS_DIR", "/FTP_1C")
REMOTE_FILE = os.getenv("FTPS_FILE", "Остатки чат-бот клиентский.xlsx")

PASV = os.getenv("PASV", "true").lower() == "true"
DISABLE_TLS_VERIFY = os.getenv("DISABLE_TLS_VERIFY", "true").lower() == "true"


class FTPSessionReuse(FTP_TLS):
    def ntransfercmd(self, cmd, rest=None):
        if self.passiveserver:
            host, port = self.makepasv()
            conn = socket.create_connection((host, port), self.timeout, self.source_address)
            if self._prot_p:
                conn = self.context.wrap_socket(
                    conn,
                    server_hostname=self.host,
                    session=getattr(self.sock, "session", None)
                )
            return conn, None
        else:
            raise OSError("Active mode not supported")


def connect_ftps():
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

    print("[FTPS] connect()…")
    ftps = FTPSessionReuse(context=ctx, timeout=120)
    ftps.connect(HOST, PORT, timeout=60)
    ftps.sock.settimeout(120)
    try:
        ftps.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    except Exception:
        pass

    print("[FTPS] auth()…")
    ftps.auth()
    print("[FTPS] login()…")
    ftps.login(USER, PASSWORD)
    print("[FTPS] prot_p()…")
    ftps.prot_p()
    ftps.set_pasv(PASV)
    ftps.encoding = "utf-8"
    return ftps


def download(ftps, remote_dir, remote_file, local_path: Path):
    ftps.cwd(remote_dir)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"[FTPS] RETR {remote_file} …")
    with NamedTemporaryFile("wb", delete=False, dir=str(local_path.parent)) as tmp:
        ftps.retrbinary(f"RETR {remote_file}", tmp.write)
        tmp_path = Path(tmp.name)
    tmp_path.replace(local_path)
    print(f"[FTPS] RETR OK → {local_path}")
    return local_path


def post_to_n8n(local_path: Path):
    if not N8N_WEBHOOK_URL:
        print("[INFO] N8N_WEBHOOK_URL not set; skip webhook")
        return
    import requests
    with open(local_path, "rb") as f:
        r = requests.post(N8N_WEBHOOK_URL, files={"file": (local_path.name, f)})
    r.raise_for_status()
    print(f"[OK] Sent to n8n webhook: {r.status_code}")


def run():
    ftps = connect_ftps()
    try:
        path = download(ftps, REMOTE_DIR, REMOTE_FILE, LOCAL_PATH)
    finally:
        try:
            ftps.quit()
        except Exception:
            pass
    post_to_n8n(LOCAL_PATH)


if __name__ == "__main__":
    run()
