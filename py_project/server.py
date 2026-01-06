import argparse
import base64
import json
import logging
import threading
import time
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import requests
import os
from storage import Storage, ErrNotFound

logger = logging.getLogger("dida")

class RequestHandler(BaseHTTPRequestHandler):
    server_version = "dida/py"

    def _set_json(self, code=200):
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._set_json(200)
            self.wfile.write(b'{"status":"ok"}')
            return
        if parsed.path.startswith("/kv/"):
            key = parsed.path[len("/kv/"):]
            try:
                v, ver = self.server.store.read(key)
            except Exception as e:
                if e == ErrNotFound:
                    self.send_error(404, "not found")
                    return
                self.send_error(500, str(e))
                return
            enc = base64.b64encode(v)
            self._set_json(200)
            self.wfile.write(json.dumps({"value": enc.decode('ascii'), "version": ver}).encode('utf-8'))
            return
        if parsed.path == "/internal/version":
            ver = self.server.store.applied_version_snapshot()
            self._set_json(200)
            self.wfile.write(json.dumps({"applied_version": ver}).encode('utf-8'))
            return
        if parsed.path == "/metrics":
            ms = self.server.store.metrics_snapshot()
            self._set_json(200)
            self.wfile.write(json.dumps(ms).encode('utf-8'))
            return
        self.send_error(404, "not found")

    def do_PUT(self):
        parsed = urlparse(self.path)
        if parsed.path.startswith("/kv/"):
            key = parsed.path[len("/kv/"):]
            length = int(self.headers.get('Content-Length', '0'))
            body = self.rfile.read(length)
            try:
                ver = self.server.store.append(key, body)
            except Exception as e:
                self.send_error(400, str(e))
                return
            self._set_json(201)
            self.wfile.write(json.dumps({"version": ver}).encode('utf-8'))
            return
        self.send_error(404, "not found")

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/internal/replicate":
            length = int(self.headers.get('Content-Length', '0'))
            body = self.rfile.read(length)
            try:
                j = json.loads(body)
                recs = j.get('records', [])
                self.server.store.apply_replication(recs)
                self._set_json(200)
                return
            except Exception as e:
                self.send_error(400, str(e))
                return
        if parsed.path == "/admin/replicate":
            q = parse_qs(parsed.query)
            target = q.get('target', [None])[0]
            if not target:
                self.send_error(400, "target required")
                return
            maxbytes = int(q.get('maxbytes', ["0"])[0])
            delayms = int(q.get('delayms', ["0"])[0])
            # get offset
            off = self.server.replication_offsets.get(target, 0)
            recs, newoff = self.server.store.replication_batch(off, maxbytes)
            if not recs:
                self.send_response(204)
                self.end_headers()
                return
            if delayms > 0:
                time.sleep(delayms / 1000.0)
            url = target.rstrip('/') + '/internal/replicate'
            try:
                r = requests.post(url, json={"records": recs}, timeout=10)
                if r.status_code != 200:
                    self.send_error(502, f"replicate failed: status={r.status_code}")
                    return
            except Exception as e:
                self.send_error(502, str(e))
                return
            self.server.replication_offsets[target] = newoff
            self._set_json(200)
            return
        self.send_error(404, "not found")

class DidaServer(ThreadingHTTPServer):
    def __init__(self, addr, store: Storage):
        super().__init__(addr, RequestHandler)
        self.store = store
        self.replication_offsets = {}

def run(addr: str, data_dir: str):
    h, p = addr.split(":") if ':' in addr else (addr, '8000')
    host = h if h else '0.0.0.0'
    port = int(p)
    store = Storage(data_dir)
    srv = DidaServer((host, port), store)
    logger.info(f"listening on {host}:{port}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        store.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--addr', default=':8080', help='listen address ("host:port" or ":port")')
    parser.add_argument('--data-dir', default='./data')
    # legacy / convenience flags accepted by some scripts
    parser.add_argument('--role', choices=['leader', 'follower'], default='leader', help='role (accepted but not used)')
    parser.add_argument('--port', type=int, help='port number (overrides port in --addr)')
    parser.add_argument('--engine', choices=['log', 'lsm'], default='log', help='storage engine hint (accepted but not used)')
    args = parser.parse_args()

    # compute final addr (port overrides --addr if provided)
    addr = args.addr
    if args.port:
        # if --addr contains a host, preserve it, otherwise use :<port>
        if ':' in addr and addr.split(':', 1)[0] != '':
            host = addr.split(':', 1)[0]
            addr = f"{host}:{args.port}"
        else:
            addr = f":{args.port}"

    logging.basicConfig(level=logging.INFO, format='[dida] %(asctime)s %(message)s')
    run(addr, args.data_dir)
