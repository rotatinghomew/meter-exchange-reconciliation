"""Standard-library JSON HTTP adapter for :class:`MeterService`."""

from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .core import MeterService, ServiceError


def _json_default(value: Any) -> Any:
    # Defensive: core normally returns plain ISO strings.
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def build_server(
    service: MeterService | None = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8000,
    seed_path: str | None = None,
) -> ThreadingHTTPServer:
    service = service or MeterService(seed_path=seed_path)

    class Handler(BaseHTTPRequestHandler):
        server_version = "MeterFlow/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:  # quieter tests
            if os.environ.get("METER_SERVICE_HTTP_LOG"):
                super().log_message(fmt, *args)

        def _actor(self) -> dict[str, Any]:
            actor_id = self.headers.get("X-Actor-Id") or self.headers.get("x-actor-id") or "system"
            scopes_header = self.headers.get("X-Actor-Scopes") or ""
            scopes = [scope.strip() for scope in scopes_header.split(",") if scope.strip()]
            return {"id": actor_id, "scopes": scopes}

        def _body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            if length == 0:
                return {}
            raw = self.rfile.read(length)
            try:
                value = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ServiceError("request body must be valid JSON", status=400) from exc
            if value is None:
                return {}
            if not isinstance(value, dict):
                raise ServiceError("JSON request body must be an object", status=400)
            return value

        def _send(self, status: int, payload: Any) -> None:
            encoded = json.dumps(
                payload, ensure_ascii=False, indent=2, default=_json_default
            ).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Actor-Id")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def _dispatch(self, method: str) -> None:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            if path.startswith("/api"):
                path = path[4:] or "/"
            query = {k: v[-1] for k, v in parse_qs(parsed.query).items()}
            actor = self._actor()
            try:
                self._route(method, path, query, actor)
            except ServiceError as exc:
                self._send(
                    exc.status,
                    {
                        "error": exc.code,
                        "message": exc.message,
                        "details": exc.details,
                    },
                )
            except Exception as exc:  # pragma: no cover - boundary guard
                self._send(500, {"error": "internal_error", "message": str(exc)})

        def do_GET(self) -> None:
            self._dispatch("GET")

        def do_POST(self) -> None:
            self._dispatch("POST")

        def do_OPTIONS(self) -> None:
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Actor-Id")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.end_headers()

        def _route(
            self, method: str, path: str, query: dict[str, str], actor: str
        ) -> None:
            if method == "GET" and path == "/health":
                self._send(200, {"ok": True, "service": "meter-flow"})
                return
            if method == "GET" and path in {"/v1/scenario", "/v1/report"}:
                self._send(200, service.scenario_report())
                return
            if method == "GET" and path == "/":
                self._send(
                    200,
                    {
                        "service": "meter-flow",
                        "endpoints": [
                            "GET /v1/scenario",
                            "POST /v1/quotes",
                            "GET /v1/requests",
                            "POST /v1/requests",
                            "GET /v1/requests/{id}/recommendation",
                            "POST /v1/requests/{id}/confirm",
                            "POST /v1/receipts",
                            "GET /v1/samples/{id}/analysis",
                            "GET /v1/work-orders",
                            "GET /v1/work-orders/{id}",
                            "POST /v1/handoffs",
                            "POST /v1/returns",
                            "POST /v1/return-cases/{id}/accept",
                            "GET /v1/corrections/{id}/impact",
                            "GET /v1/receipts/{id}?view=summary|original|readings|corrected",
                            "GET /v1/reviews",
                            "GET /v1/reviews/{id}",
                            "GET /v1/reviews/{id}/routing",
                            "POST /v1/reviews/{id}/propose",
                            "POST /v1/reviews/{id}/confirm",
                            "GET /v1/confirmations/{id}",
                            "GET /v1/actors/{id}/tasks",
                        ],
                    },
                )
                return

            if path == "/v1/quotes" and method == "POST":
                self._send(200, service.quote_requirement(self._body()))
                return
            if method == "GET" and path in {"/v1/quotes", "/v1/availability"}:
                try:
                    raw_range = query.get("range")
                    if raw_range and "," in raw_range:
                        lo_raw, hi_raw = raw_range.split(",", 1)
                        range_value = [float(lo_raw), float(hi_raw)]
                    else:
                        range_value = [
                            float(query.get("lo", query.get("range_lo"))),
                            float(query.get("hi", query.get("range_hi"))),
                        ]
                except (TypeError, ValueError):
                    self._send(
                        400,
                        {"error": "invalid_range_query", "message": "use range=lo,hi or lo and hi"},
                    )
                    return
                payload = {
                    "site": query.get("site") or query.get("usage_point"),
                    "company": query.get("company"),
                    "medium": query.get("medium"),
                    "flow_unit": query.get("flow_unit"),
                    "total_unit": query.get("total_unit"),
                    "range": range_value,
                    "start": query.get("start") or query.get("expected_start"),
                    "end": query.get("end") or query.get("expected_end"),
                }
                self._send(200, service.quote_requirement(payload))
                return
            if path == "/v1/requests" and method == "GET":
                self._send(200, service.list_requests())
                return
            if path == "/v1/requests" and method == "POST":
                self._send(201, service.submit_request(self._body()))
                return
            if path == "/v1/receipts" and method == "POST":
                self._send(201, service.issue_receipt(self._body(), actor=actor))
                return
            if path == "/v1/receipts" and method == "GET":
                self._send(200, service.records.get("receipts", []))
                return

            if path == "/v1/samples" and method == "GET":
                self._send(200, service.records.get("samples", []))
                return

            if path == "/v1/work-orders" and method == "GET":
                self._send(200, service.list_work_orders())
                return

            if path == "/v1/handoffs" and method == "POST":
                self._send(201, service.record_handoff(self._body(), actor=actor))
                return
            if path == "/v1/returns" and method == "POST":
                self._send(201, service.record_return(self._body(), actor=actor))
                return
            if path == "/v1/certificates" and method == "POST":
                self._send(201, service.add_certificate(self._body(), actor=actor))
                return
            if path == "/v1/certificate-revisions" and method == "POST":
                self._send(201, service.add_revision(self._body(), actor=actor))
                return

            if path == "/v1/reviews" and method == "GET":
                self._send(200, service.list_review_cases(actor))
                return

            parts = [p for p in path.split("/") if p]
            # Expected forms are handled explicitly to keep the adapter readable.
            if len(parts) >= 3 and parts[0] == "v1":
                resource = parts[1]
                resource_id = parts[2]
                action = parts[3] if len(parts) >= 4 else None

                if resource == "requests":
                    if action is None and method == "GET":
                        self._send(200, service.get_request(resource_id))
                        return
                    if action in {"recommendation", "quote"} and method == "GET":
                        self._send(200, service.recommend_for_request(resource_id))
                        return
                    if action in {"confirm", "execute", "allocate"} and method == "POST":
                        self._send(
                            200,
                            service.confirm_request(
                                resource_id, self._body(), actor=actor
                            ),
                        )
                        return

                if resource == "work-orders" and action is None and method == "GET":
                    self._send(200, service.get_work_order(resource_id))
                    return

                if resource == "samples" and action == "analysis" and method == "GET":
                    self._send(200, service.analyze_sample(resource_id))
                    return

                if resource == "return-cases" and action == "accept" and method == "POST":
                    self._send(200, service.accept_return(resource_id, self._body(), actor=actor))
                    return

                if resource == "meters" and action is None and method == "GET":
                    self._send(200, service.meter_timeline(resource_id))
                    return

                if resource == "meters" and action == "probe" and method == "POST":
                    body = self._body()
                    self._send(
                        200,
                        service.replace_probe(
                            resource_id,
                            body.get("probe") or body.get("new_probe"),
                            at=body.get("at"),
                            actor=actor,
                        ),
                    )
                    return

                if resource == "corrections" and action == "impact" and method == "GET":
                    self._send(200, service.correction_impact(resource_id))
                    return

                if resource == "receipts" and method == "GET":
                    view = query.get("view", "summary")
                    self._send(200, service.get_receipt_view(resource_id, view, actor))
                    return

                if resource == "reviews":
                    if action == "routing" and method == "GET":
                        self._send(200, service.get_review_routing(resource_id))
                        return
                    if action is None and method == "GET":
                        if query.get("view") == "routing":
                            self._send(200, service.get_review_routing(resource_id))
                        else:
                            self._send(200, service.get_review_case(resource_id, actor))
                        return
                    if action == "propose" and method == "POST":
                        self._send(
                            200,
                            service.propose_review(
                                resource_id, self._body(), actor=actor
                            ),
                        )
                        return
                    if action == "confirm" and method == "POST":
                        self._send(
                            200,
                            service.confirm_review(
                                resource_id, self._body(), actor=actor
                            ),
                        )
                        return

                if resource == "confirmations" and action is None and method == "GET":
                    self._send(200, service.get_confirmation(resource_id, actor))
                    return

                if resource == "actors" and action == "tasks" and method == "GET":
                    self._send(200, service.actor_tasks(resource_id))
                    return

            self._send(404, {"error": "route_not_found", "path": path})

    return ThreadingHTTPServer((host, port), Handler)


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Meter qualification and review service")
    parser.add_argument("--host", default=os.environ.get("HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8000")))
    parser.add_argument("--seed", default=None)
    parser.add_argument("--demo", action="store_true", help="print the local scenario report and exit")
    args = parser.parse_args(argv)

    service = MeterService(seed_path=args.seed)
    if args.demo:
        print(json.dumps(service.scenario_report(), ensure_ascii=False, indent=2))
        return 0

    server = build_server(service, host=args.host, port=args.port)
    host, port = server.server_address[:2]
    print(f"meter-flow service listening on http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
