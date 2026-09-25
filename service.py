"""考古现场关系管理服务入口。

除原有 /health 外，提供事件摄入与只读视图。所有写操作均为事件，
服务本身不保存可变业务状态——状态是事件日志的确定性投影。
"""

import argparse
import json
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import fieldwork as fw

SERVICE_ID = "archaeology-fieldwork"
SERVICE_NAME = "考古现场关系管理"

STORE = fw.FieldworkStore()


def reset_store(journal_path: str | None = None) -> None:
    """重置内存存储（供测试隔离；生产代码不应调用）。"""
    global STORE
    STORE = fw.FieldworkStore(journal_path)


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


class Handler(BaseHTTPRequestHandler):
    """HTTP 路由：GET 只读视图，POST /events 摄入事件。"""

    def _send(self, status_code: int, payload) -> None:
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # -- GET ----------------------------------------------------------------

    def do_GET(self):
        parsed = urlparse(self.path)
        route, query = parsed.path.rstrip("/") or "/", parse_qs(parsed.query)
        try:
            if route == "/health":
                self._send(200, health_payload())
            elif route == "/events":
                log = STORE.physical_log()
                self._send(200, {
                    "count": len(log),
                    "events": [
                        {**event, "_status": STORE.status_of(event["event_id"])}
                        for event in sorted(log, key=fw.canonical_key)
                    ],
                })
            elif route == "/records":
                self._send(200, {"records": fw.build_views(STORE)["records"]})
            elif route == "/records/history":
                record_id = query.get("record_id", [None])[0]
                self._send(200, _record_history(record_id))
            elif route == "/relations":
                self._send(200, {
                    **fw.spatial_closure(STORE.projection),
                    "labeled": fw.build_views(STORE)["relations"],
                })
            elif route == "/samples":
                self._send(200, {"samples": _samples_view(query)})
            elif route == "/loans":
                self._send(200, _loans_view(query))
            elif route == "/custody":
                self._send(200, {"items": fw.build_views(STORE)["custody"]})
            elif route == "/datings":
                views = fw.build_views(STORE)
                self._send(200, {
                    "active": views["datings"],
                    "withdrawn": views["datings_withdrawn"],
                })
            elif route == "/claims":
                self._send(200, {"claims": fw.build_views(STORE)["claims"]})
            elif route == "/releases":
                self._send(200, {"releases": fw.build_views(STORE)["releases"]})
            elif route == "/conflicts":
                self._send(200, fw.build_views(STORE)["conflicts"])
            elif route == "/verify/chains":
                self._send(200, {"devices": STORE.verify_chains()})
            elif route == "/asof":
                self._send(200, _as_of(query))
            else:
                self.send_error(404)
        except fw.FieldworkError as exc:
            self._send(400, {"error": str(exc)})

    # -- POST ---------------------------------------------------------------

    def do_POST(self):
        parsed = urlparse(self.path)
        route = parsed.path.rstrip("/") or "/"
        if route != "/events":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            body = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            self._send(400, {"error": f"请求体不是合法 JSON: {exc}"})
            return

        # 允许 {"events": [...]} 批量，或单个事件对象。
        if isinstance(body, dict) and "event_id" in body:
            events = [body]
        elif isinstance(body, dict) and isinstance(body.get("events"), list):
            events = body["events"]
        else:
            self._send(400, {"error": "请求体须为事件对象或 {\"events\": [...]}"})
            return

        results = STORE.ingest_batch(events)
        status_code = 207 if any(r["status"] in ("rejected", "quarantined")
                                 for r in results) else 200
        # waiting 不算失败：缺环归队后会自愈。
        self._send(status_code, {"results": results})

    def log_message(self, *_args):
        return


def _observe_at(query):
    """从查询参数取观察时点；缺省以当前时刻派生逾期标记。"""
    raw = query.get("as_of", [None])[0]
    if raw:
        return fw.parse_occurred_at(raw)
    return datetime.now()


def _samples_view(query):
    return fw.build_views(STORE, as_of=_observe_at(query))["samples"]


def _loans_view(query):
    """借用谱系视图；支持 sample_id/status 过滤与 as_of 指定观察日期。"""
    cutoff = _observe_at(query)
    views = fw.build_views(STORE, as_of=cutoff)
    loans = views["loans"]
    sample_id = query.get("sample_id", [None])[0]
    if sample_id:
        loans = [loan for loan in loans if loan["sample_id"] == sample_id]
    status_filter = query.get("status", [None])[0]
    if status_filter:
        loans = [loan for loan in loans if loan["status"] == status_filter]
    return {
        "loans": loans,
        "count": len(loans),
        "samples": {s["sample_id"]: s for s in views["samples"]},
    }


def _record_history(record_id):
    """返回记录的全部版本（原值保留），支持逐版追溯。"""
    if not record_id:
        raise fw.FieldworkError("需要 record_id 查询参数")
    record = STORE.projection.records.get(record_id)
    if record is None:
        return {"record_id": record_id, "found": False, "versions": []}
    return {
        "record_id": record_id,
        "found": True,
        "kind": record["kind"],
        "label": record["label"],
        "numbers": sorted(record["numbers"]),
        "versions": record["versions"],
    }


def _as_of(query):
    raw = query.get("date", [None])[0]
    if not raw:
        raise fw.FieldworkError("需要 date 查询参数（ISO8601）")
    cutoff = fw.parse_occurred_at(raw)
    return fw.state_as_of(STORE, cutoff)


def main():
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--store", help="JSONL 事件日志路径，启动时加载并追加写入")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        assert health_payload()["service"] == SERVICE_ID
        assert callable(fw.canonical_json)
        print("基础检查通过")
        return
    if args.store:
        STORE.journal_path = args.store
        try:
            summary = STORE.load_jsonl(args.store)
            print(f"已加载事件 {summary['loaded']} 条：{args.store}")
        except FileNotFoundError:
            open(args.store, "a", encoding="utf-8").close()
    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
