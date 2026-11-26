"""Core stateful domain service for meter qualification and evidence review.

The module intentionally uses only the Python standard library.  Time values are
timezone-aware ISO-8601 strings and intervals are treated as ``[start, end)``.
Flow ranges are physical/calibration ranges and are therefore handled as closed
intervals.
"""

from __future__ import annotations

import copy
import json
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SEED = ROOT / "data" / "source_records.json"

BLOCKING_ALLOCATION_STATUSES = {
    "confirmed",
    "executable",
    "ready",
    "checked_out",
    "active",
    "temporarily_replaced",
    "in_progress",
}


class ServiceError(Exception):
    """Base class for expected service errors."""

    status = 400
    code = "service_error"

    def __init__(
        self,
        message: str,
        *,
        details: Any | None = None,
        status: int | None = None,
        code: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.details = details
        if status is not None:
            self.status = status
        if code is not None:
            self.code = code


class ValidationError(ServiceError):
    status = 422
    code = "validation_error"


class NotFoundError(ServiceError):
    status = 404
    code = "not_found"


class ConflictError(ServiceError):
    status = 409
    code = "conflict"


class AuthorizationError(ServiceError):
    status = 403
    code = "authorization_error"


def load_seed(path: str | Path | None = None) -> dict[str, Any]:
    seed_path = Path(path) if path is not None else DEFAULT_SEED
    return json.loads(seed_path.read_text(encoding="utf-8"))


def parse_dt(value: Any, field: str = "timestamp") -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValidationError(f"{field} is not a valid ISO-8601 timestamp: {value}") from exc
    else:
        raise ValidationError(f"{field} must be an ISO-8601 string or datetime")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError(f"{field} must include an explicit UTC offset")
    return parsed


def iso(value: datetime) -> str:
    return value.isoformat()


def dt_overlap(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return max(a_start, b_start) < min(a_end, b_end)


def ranges_overlap(a: Iterable[float], b: Iterable[float]) -> bool:
    a0, a1 = list(a)
    b0, b1 = list(b)
    return max(a0, b0) <= min(a1, b1)


def range_contains(outer: Iterable[float], inner: Iterable[float]) -> bool:
    o0, o1 = list(outer)
    i0, i1 = list(inner)
    return o0 <= i0 and i1 <= o1


def actor_id(actor: Any | None) -> str:
    if actor is None:
        return "system"
    if isinstance(actor, str):
        return actor
    if isinstance(actor, dict):
        return str(actor.get("id") or actor.get("actor_id") or actor.get("name") or "system")
    if hasattr(actor, "id"):
        return str(getattr(actor, "id"))
    return str(actor)


class MeterService:
    """Thread-safe in-process service.

    A :class:`threading.RLock` protects every read-modify-write operation.
    Read operations also take the lock so HTTP worker threads observe a coherent
    snapshot while a confirmation is in progress.
    """

    def __init__(
        self,
        data: dict[str, Any] | str | Path | None = None,
        *,
        seed_path: str | Path | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if isinstance(data, (str, Path)):
            seed_path = data
            data = None
        if data is None:
            data = load_seed(seed_path)
        self.records: dict[str, Any] = copy.deepcopy(data)
        self._clock = clock or (lambda: datetime.now().astimezone())
        self._lock = threading.RLock()
        self.review_states: dict[str, dict[str, Any]] = {}
        self.confirmations: dict[str, dict[str, Any]] = {}
        self._bootstrap_operational_records()

    # ------------------------------------------------------------------
    # Initialisation and generic helpers
    # ------------------------------------------------------------------

    def now(self) -> datetime:
        return self._clock()

    def _new_id(self, prefix: str, collection: Iterable[dict[str, Any]], key: str = "id") -> str:
        max_n = 0
        for item in collection:
            value = str(item.get(key, ""))
            if value.startswith(prefix):
                tail = value[len(prefix):]
                try:
                    max_n = max(max_n, int(tail))
                except ValueError:
                    pass
        return f"{prefix}{max_n + 1}"

    def _bootstrap_operational_records(self) -> None:
        records = self.records
        records.setdefault("allocations", [])
        records.setdefault("work_orders", [])
        records.setdefault("return_cases", [])
        records.setdefault("probe_installations", [])
        records.setdefault("audit_events", [])

        # The exchange file stores H2 in the same handoffs list.  The business
        # rule cares that the field return record and warehouse acceptance are
        # separate sources, so normalise them without discarding the original.
        known_return_sources = {
            event.get("source_event_id")
            for case in records["return_cases"]
            for event in [case.get("return_event"), case.get("acceptance_event")]
            if event
        }
        for handoff in records.get("handoffs", []):
            if not handoff.get("meter") or not handoff.get("returned_at"):
                continue
            return_source_id = f"RR-{handoff['id']}"
            if return_source_id in known_return_sources:
                continue
            case: dict[str, Any] = {
                "id": f"RC-{handoff['id']}",
                "meter": handoff["meter"],
                "source_handoff_id": handoff["id"],
                "return_event": {
                    "id": return_source_id,
                    "at": handoff["returned_at"],
                    "source": "field_return_record",
                },
            }
            if handoff.get("accepted_at"):
                case["acceptance_event"] = {
                    "id": f"WA-{handoff['id']}",
                    "at": handoff["accepted_at"],
                    "source": "warehouse_acceptance",
                }
            records["return_cases"].append(case)

        # Build the known W1 meter chain from its site handoff.  This is a
        # projection of facts already present in the exchange record; it does
        # not invent readings or quantities.
        if not records["allocations"]:
            for request in records.get("requests", []):
                try:
                    req = self._request_requirement(request)
                except ServiceError:
                    request.setdefault("status", "invalid_request")
                    continue
                site_handoffs = sorted(
                    [
                        h
                        for h in records.get("handoffs", [])
                        if h.get("site") == req["site"]
                        and h.get("old_meter")
                        and h.get("new_meter")
                        and not (
                            parse_dt(h["at"]) <= req["start"]
                            or parse_dt(h["at"]) >= req["end"]
                        )
                    ],
                    key=lambda h: h["at"],
                )
                if not site_handoffs:
                    request.setdefault("status", "open")
                    continue

                segments: list[dict[str, Any]] = []
                cursor = req["start"]
                current_meter = site_handoffs[0]["old_meter"]
                for handoff in site_handoffs:
                    at = parse_dt(handoff["at"])
                    segments.append(
                        self._make_segment_ref(
                            current_meter,
                            cursor,
                            at,
                            handoff_out=handoff["id"],
                        )
                    )
                    cursor = at
                    current_meter = handoff["new_meter"]
                segments.append(
                    self._make_segment_ref(
                        current_meter,
                        cursor,
                        req["end"],
                        handoff_in=site_handoffs[-1]["id"],
                    )
                )
                allocation_id = f"A-{request['id']}"
                allocation = {
                    "id": allocation_id,
                    "request_id": request["id"],
                    "company": request.get("company"),
                    "site": req["site"],
                    "medium": req["medium"],
                    "range": request.get("range"),
                    "start": iso(req["start"]),
                    "end": iso(req["end"]),
                    "status": "fulfilled",
                    "confirmed": True,
                    "historical": True,
                    "segments": segments,
                }
                records["allocations"].append(allocation)
                work_order = {
                    "id": f"WO-{request['id']}",
                    "request_id": request["id"],
                    "allocation_id": allocation_id,
                    "company": request.get("company"),
                    "site": req["site"],
                    "medium": req["medium"],
                    "range": request.get("range"),
                    "start": iso(req["start"]),
                    "end": iso(req["end"]),
                    "status": "completed",
                    "segments": copy.deepcopy(segments),
                }
                records["work_orders"].append(work_order)
                request["status"] = "fulfilled"
                request["allocation_id"] = allocation_id
                request["work_order_id"] = work_order["id"]

    def _make_segment_ref(
        self,
        meter_id: str,
        start: datetime,
        end: datetime,
        *,
        handoff_in: str | None = None,
        handoff_out: str | None = None,
    ) -> dict[str, Any]:
        meter = self.get_meter(meter_id)
        cert = self._current_cert_for_interval(
            meter_id,
            meter.get("probe"),
            start,
            end,
        )
        return {
            "meter": meter_id,
            "probe": meter.get("probe"),
            "certificate_id": cert["id"] if cert else None,
            "from": iso(start),
            "to": iso(end),
            "handoff_in": handoff_in,
            "handoff_out": handoff_out,
        }

    # ------------------------------------------------------------------
    # Lookup and normalisation
    # ------------------------------------------------------------------

    def get_meter(self, meter_id: str) -> dict[str, Any]:
        for meter in self.records.get("meters", []):
            if meter["meter"] == meter_id:
                return meter
        raise NotFoundError(f"meter not found: {meter_id}", code="meter_not_found")

    def _find(
        self,
        collection: str,
        item_id: str,
        *,
        id_key: str = "id",
    ) -> dict[str, Any]:
        for item in self.records.get(collection, []):
            if item.get(id_key) == item_id:
                return item
        raise NotFoundError(f"{collection[:-1]} not found: {item_id}")

    def get_request(self, request_id: str) -> dict[str, Any]:
        return self._find("requests", request_id)

    def get_receipt(
        self,
        receipt_id: str,
        actor: Any | None = None,
        required_scope: str | None = None,
    ) -> dict[str, Any]:
        receipt = self._find("receipts", receipt_id)
        self._authorize_receipt(receipt, actor, required_scope)
        return receipt

    def get_agreement(self, agreement_id: str) -> dict[str, Any]:
        return self._find("agreements", agreement_id)

    def _authorize_receipt(
        self,
        receipt: dict[str, Any],
        actor: Any | None,
        required_scope: str | None = None,
    ) -> dict[str, Any]:
        agreement = self.get_agreement(receipt["agreement"])
        who = actor_id(actor)
        # "system" is the trusted local/direct-call operator.  HTTP callers may
        # assert a business actor through X-Actor-Id; rental-library is not one
        # of the two confirmation parties for a past receipt.
        if who in {"system", "local-operator"}:
            return agreement
        if who in {agreement["issuer"], agreement["counterparty"]}:
            return agreement
        scopes = actor.get("scopes", []) if isinstance(actor, dict) else []
        if required_scope and (
            required_scope in scopes
            or "receipt:*" in scopes
            or "evidence:*" in scopes
            or "*" in scopes
        ):
            return agreement
        raise AuthorizationError(
            "only the issuer and counterparty of the agreement may access this evidence",
            details={
                "agreement": agreement["id"],
                "required_scope": required_scope,
                "responsible_parties": [agreement["issuer"], agreement["counterparty"]],
            },
        )

    def _require_review_party(
        self, case: dict[str, Any], actor: Any | None
    ) -> tuple[str, dict[str, Any]]:
        who = actor_id(actor)
        agreement = self.get_agreement(case["agreement_id"])
        if who in {"system", "local-operator"}:
            # Direct system calls may prepare a proposal, but a confirmation
            # still records both business parties below.
            return who, agreement
        if who not in {agreement["issuer"], agreement["counterparty"]}:
            raise AuthorizationError(
                "past voucher review must be confirmed by the using parties, not the rental library",
                details=case["responsible_parties"],
            )
        return who, agreement

    def _normalize_requirement(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValidationError("JSON request body must be an object")
        site = payload.get("site") or payload.get("usage_point") or payload.get("point")
        medium = payload.get("medium")
        raw_range = payload.get("range") or payload.get("required_range")
        start_raw = payload.get("start") or payload.get("expected_start") or payload.get("start_at")
        end_raw = payload.get("end") or payload.get("expected_end") or payload.get("end_at")
        if not site:
            raise ValidationError("site/usage_point is required")
        if not medium:
            raise ValidationError("medium is required")
        if not isinstance(raw_range, list) or len(raw_range) != 2:
            raise ValidationError("range/required_range must be [lower, upper]")
        try:
            lo = float(raw_range[0])
            hi = float(raw_range[1])
        except (TypeError, ValueError) as exc:
            raise ValidationError("range bounds must be numbers") from exc
        if lo >= hi:
            raise ValidationError("range lower bound must be less than upper bound")
        start = parse_dt(start_raw, "start")
        end = parse_dt(end_raw, "end")
        if start >= end:
            raise ValidationError("start must be before end")
        flow_unit = payload.get("flow_unit")
        total_unit = payload.get("total_unit") or payload.get("unit")
        return {
            "site": str(site),
            "company": payload.get("company") or payload.get("requester"),
            "medium": str(medium),
            "flow_unit": flow_unit,
            "total_unit": total_unit,
            "range": [lo, hi],
            "start": start,
            "end": end,
        }

    def _request_requirement(self, request: dict[str, Any]) -> dict[str, Any]:
        return self._normalize_requirement(
            {
                "site": request.get("site"),
                "company": request.get("company"),
                "medium": request.get("medium"),
                "flow_unit": request.get("flow_unit"),
                "total_unit": request.get("total_unit"),
                "range": request.get("range"),
                "start": request.get("start"),
                "end": request.get("end"),
            }
        )

    # ------------------------------------------------------------------
    # Certificates and qualification
    # ------------------------------------------------------------------

    def _cert_matches_combo(self, cert: dict[str, Any], meter_id: str, probe: str) -> bool:
        return cert.get("meter") == meter_id and cert.get("probe") == probe

    def _current_cert_for_interval(
        self,
        meter_id: str,
        probe: str | None,
        start: datetime,
        end: datetime,
        flow_range: list[float] | None = None,
    ) -> dict[str, Any] | None:
        candidates = []
        for cert in self.records.get("certificates", []):
            if not self._cert_matches_combo(cert, meter_id, probe or ""):
                continue
            valid_from = parse_dt(cert["from"])
            valid_to = parse_dt(cert["to"])
            if valid_from <= start and end <= valid_to:
                if flow_range is None or range_contains(cert["range"], flow_range):
                    candidates.append(cert)
        if not candidates:
            return None
        # Prefer the narrowest sufficient range; deterministic for tests.
        return sorted(candidates, key=lambda c: (c["range"][1] - c["range"][0], c["id"]))[0]

    def meter_role(self, meter_id: str) -> str:
        reference_samples = [
            s
            for s in self.records.get("samples", [])
            if s.get("meter") == meter_id and s.get("reference") is True
        ]
        all_meter_samples = [s for s in self.records.get("samples", []) if s.get("meter") == meter_id]
        if reference_samples and len(reference_samples) == len(all_meter_samples):
            return "reference_standard"
        return "working"

    def _revision_hits(
        self,
        meter_id: str,
        probe: str | None,
        start: datetime,
        end: datetime,
        required_range: list[float],
        cert_id: str | None = None,
    ) -> list[dict[str, Any]]:
        hits = []
        for rev in self.records.get("certificate_revision", []):
            cert = next(
                (c for c in self.records["certificates"] if c["id"] == rev.get("cert")),
                None,
            )
            if cert_id is not None and rev.get("cert") != cert_id:
                continue
            if cert is not None and not self._cert_matches_combo(cert, meter_id, probe or ""):
                continue
            if cert is None:
                if rev.get("meter") and rev.get("meter") != meter_id:
                    continue
                if rev.get("probe") and rev.get("probe") != probe:
                    continue
            rev_start = parse_dt(rev["effective_from"])
            rev_end = parse_dt(rev["effective_to"])
            if not dt_overlap(start, end, rev_start, rev_end):
                continue
            if not ranges_overlap(required_range, rev["affected_range"]):
                continue
            hits.append(
                {
                    "revision_id": rev["id"],
                    "certificate_id": rev.get("cert"),
                    "probe": rev.get("probe"),
                    "effective_from": rev["effective_from"],
                    "effective_to": rev["effective_to"],
                    "affected_range": rev["affected_range"],
                    "reported_at": rev.get("reported_at"),
                }
            )
        return hits

    def _custody_windows(self, meter_id: str) -> list[dict[str, Any]]:
        windows = []
        for case in self.records.get("return_cases", []):
            if case["meter"] != meter_id:
                continue
            returned = parse_dt(case["return_event"]["at"])
            acceptance = case.get("acceptance_event")
            accepted = parse_dt(acceptance["at"]) if acceptance else None
            # If the two independent sources disagree, the interval between
            # them is not safe to promise to another rental.
            if accepted is None:
                start, end = returned, None
            else:
                start, end = min(returned, accepted), max(returned, accepted)
            windows.append(
                {
                    "return_case_id": case["id"],
                    "start": iso(start),
                    "end": iso(end) if end else None,
                    "_start_dt": start,
                    "_end_dt": end,
                    "returned_at": iso(returned),
                    "accepted_at": iso(accepted) if accepted else None,
                    "return_source": case["return_event"]["source"],
                    "acceptance_source": acceptance["source"] if acceptance else None,
                }
            )
        return windows

    def _allocation_windows(
        self, meter_id: str, *, exclude_allocation_id: str | None = None
    ) -> list[dict[str, Any]]:
        windows = []
        for allocation in self.records.get("allocations", []):
            if allocation["id"] == exclude_allocation_id:
                continue
            if allocation.get("status") not in BLOCKING_ALLOCATION_STATUSES:
                # Historical fulfilled allocations still represent confirmed
                # occupancy over their original interval.
                if not allocation.get("historical"):
                    continue
            for segment in allocation.get("segments", []):
                if segment.get("meter") != meter_id:
                    continue
                windows.append(
                    {
                        "start": parse_dt(segment["from"]),
                        "end": parse_dt(segment["to"]),
                        "allocation_id": allocation["id"],
                        "request_id": allocation.get("request_id"),
                        "status": allocation.get("status"),
                    }
                )
        return windows

    def _next_delivery_window(
        self,
        meter_id: str,
        req: dict[str, Any],
        *,
        exclude_allocation_id: str | None = None,
    ) -> dict[str, Any]:
        busy: list[tuple[datetime, datetime | None, str]] = []
        for window in self._custody_windows(meter_id):
            busy.append((window["_start_dt"], window["_end_dt"], "return_acceptance_gap"))
        for window in self._allocation_windows(
            meter_id, exclude_allocation_id=exclude_allocation_id
        ):
            busy.append((window["start"], window["end"], window["allocation_id"]))

        cursor = req["start"]
        duration = req["end"] - req["start"]
        advanced_reasons: list[str] = []
        # Find the earliest start at which a full requested-duration interval
        # does not overlap a booking or an unresolved return/acceptance gap.
        while True:
            advance_to = None
            blocking_reason = None
            for start, end, reason in busy:
                if end is None:
                    if start < cursor + duration:
                        return {
                            "requested_start": iso(req["start"]),
                            "requested_end": iso(req["end"]),
                            "earliest_delivery_at": None,
                            "reason": "awaiting_warehouse_acceptance",
                        }
                    continue
                if dt_overlap(cursor, cursor + duration, start, end):
                    candidate_end = end
                    if advance_to is None or candidate_end > advance_to:
                        advance_to = candidate_end
                        blocking_reason = reason
            if advance_to is None:
                break
            cursor = advance_to
            if blocking_reason:
                advanced_reasons.append(blocking_reason)
        fixed_end_seconds = max(0.0, (req["end"] - cursor).total_seconds())
        return {
            "requested_start": iso(req["start"]),
            "requested_end": iso(req["end"]),
            "earliest_delivery_at": iso(cursor),
            "next_full_duration_window": [iso(cursor), iso(cursor + duration)],
            "usable_until_original_end": [
                iso(cursor),
                iso(req["end"]) if cursor < req["end"] else iso(cursor),
            ],
            "available_seconds_before_original_end": fixed_end_seconds,
            "delay_seconds": max(0.0, (cursor - req["start"]).total_seconds()),
            "blocked_by": sorted(set(advanced_reasons)),
        }

    def _subtract_affected_ranges(
        self, base: list[float], hits: list[dict[str, Any]]
    ) -> list[list[float]]:
        ranges = [list(base)]
        for hit in hits:
            cut_lo, cut_hi = hit["affected_range"]
            next_ranges: list[list[float]] = []
            for lo, hi in ranges:
                if cut_hi < lo or hi < cut_lo:
                    next_ranges.append([lo, hi])
                    continue
                if lo < cut_lo:
                    next_ranges.append([lo, cut_lo])
                if cut_hi < hi:
                    next_ranges.append([cut_hi, hi])
            ranges = next_ranges
        return ranges

    def assess_meter(
        self,
        meter_id: str,
        requirement: dict[str, Any],
        *,
        probe: str | None = None,
        exclude_allocation_id: str | None = None,
    ) -> dict[str, Any]:
        meter = self.get_meter(meter_id)
        probe = probe or meter.get("probe")
        reasons: list[str] = []
        details: dict[str, Any] = {}

        if requirement.get("medium") and meter.get("medium") != requirement["medium"]:
            reasons.append("medium_mismatch")
            details["meter_medium"] = meter.get("medium")
        if requirement.get("flow_unit") and meter.get("flow_unit") != requirement["flow_unit"]:
            reasons.append("flow_unit_mismatch")
        if requirement.get("total_unit") and meter.get("total_unit") != requirement["total_unit"]:
            reasons.append("total_unit_mismatch")
        if not range_contains(meter["range"], requirement["range"]):
            reasons.append("instrument_range_insufficient")

        cert = self._current_cert_for_interval(
            meter_id,
            probe,
            requirement["start"],
            requirement["end"],
            requirement["range"],
        )
        hits: list[dict[str, Any]] = []
        restriction_hits: list[dict[str, Any]] = []
        if cert is None:
            any_combo_cert = [
                c for c in self.records["certificates"] if self._cert_matches_combo(c, meter_id, probe)
            ]
            if not any_combo_cert:
                reasons.append("no_certificate_for_exact_meter_probe_combination")
            else:
                reasons.append("certificate_does_not_cover_time_or_range")
            details["same_model_certificate_is_not_transferable"] = True
        else:
            hits = self._revision_hits(
                meter_id,
                probe,
                requirement["start"],
                requirement["end"],
                requirement["range"],
                cert["id"],
            )
            if hits:
                reasons.append("certificate_revision_affects_requested_use")
                details["revision_hits"] = hits
            restriction_hits = self._revision_hits(
                meter_id,
                probe,
                requirement["start"],
                requirement["end"],
                cert["range"],
                cert["id"],
            )

        custody_conflicts = []
        for window in self._custody_windows(meter_id):
            start_dt = window["_start_dt"]
            end_dt = window["_end_dt"]
            is_conflict = (
                end_dt is None and start_dt <= requirement["end"]
            ) or (
                end_dt is not None
                and dt_overlap(requirement["start"], requirement["end"], start_dt, end_dt)
            )
            if is_conflict:
                custody_conflicts.append(
                    {k: v for k, v in window.items() if not k.startswith("_")}
                )
        if custody_conflicts:
            reasons.append("return_acceptance_source_gap")
            details["custody_conflicts"] = custody_conflicts

        booking_conflicts = []
        for window in self._allocation_windows(
            meter_id, exclude_allocation_id=exclude_allocation_id
        ):
            if dt_overlap(
                requirement["start"], requirement["end"], window["start"], window["end"]
            ):
                booking_conflicts.append(window)
        if booking_conflicts:
            reasons.append("confirmed_allocation_conflict")
            details["booking_conflicts"] = [
                {
                    **window,
                    "start": iso(window["start"]),
                    "end": iso(window["end"]),
                }
                for window in booking_conflicts
            ]

        delivery = self._next_delivery_window(
            meter_id, requirement, exclude_allocation_id=exclude_allocation_id
        )
        eligible = not reasons
        return {
            "meter": meter_id,
            "probe": probe,
            "model": meter.get("model"),
            "role": self.meter_role(meter_id),
            "site_recorded": meter.get("site"),
            "actual_available_range": meter.get("range"),
            "instrument_range": meter.get("range"),
            "certified_range": cert.get("range") if cert else None,
            "effective_usable_ranges": (
                self._subtract_affected_ranges(cert["range"], restriction_hits) if cert else []
            ),
            "range_boundary_note": "correction affected_range endpoints are treated as affected; displayed cut points are conservative",
            "certificate": (
                {
                    "id": cert["id"],
                    "meter": cert["meter"],
                    "probe": cert["probe"],
                    "range": cert["range"],
                    "from": cert["from"],
                    "to": cert["to"],
                }
                if cert
                else None
            ),
            "eligible": eligible,
            "reasons": reasons,
            "details": details,
            "delivery_window": delivery,
        }

    def quote_requirement(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            requirement = self._normalize_requirement(payload)
            requested_meter = payload.get("meter") or payload.get("meter_id")
            requested_probe = payload.get("probe")
            include_reference = bool(payload.get("include_reference", False))

            if requested_meter:
                candidates = [
                    self.assess_meter(
                        requested_meter,
                        requirement,
                        probe=requested_probe,
                    )
                ]
            else:
                candidates = []
                for meter in self.records.get("meters", []):
                    if self.meter_role(meter["meter"]) == "reference_standard" and not include_reference:
                        continue
                    candidates.append(
                        self.assess_meter(
                            meter["meter"],
                            requirement,
                            probe=requested_probe if meter["meter"] == requested_meter else None,
                        )
                    )

            candidates.sort(
                key=lambda c: (
                    0 if c["eligible"] else 1,
                    c["actual_available_range"][1] - c["actual_available_range"][0],
                    c["meter"],
                )
            )
            recommended = next((c for c in candidates if c["eligible"]), None)
            return {
                "requirement": self._public_requirement(requirement),
                "candidates": candidates,
                "recommended": recommended,
                "recommended_instrument_set": self._instrument_set(recommended)
                if recommended
                else None,
            }

    def _instrument_set(self, candidate: dict[str, Any]) -> dict[str, Any]:
        if not candidate:
            return None
        return {
            "meter": candidate["meter"],
            "host_serial": candidate["meter"],
            "probe": candidate["probe"],
            "model": candidate["model"],
            "certificate_id": candidate["certificate"]["id"] if candidate["certificate"] else None,
            "actual_available_range": candidate["actual_available_range"],
            "delivery_window": candidate["delivery_window"],
        }

    def _public_requirement(self, req: dict[str, Any]) -> dict[str, Any]:
        return {
            "site": req["site"],
            "company": req.get("company"),
            "medium": req["medium"],
            "flow_unit": req.get("flow_unit"),
            "total_unit": req.get("total_unit"),
            "range": req["range"],
            "start": iso(req["start"]),
            "end": iso(req["end"]),
        }

    def recommend_for_request(self, request_id: str) -> dict[str, Any]:
        with self._lock:
            request = self.get_request(request_id)
            requirement = self._request_requirement(request)
            quote = self.quote_requirement(
                {
                    **self._public_requirement(requirement),
                    "meter": None,
                }
            )
            quote["request_id"] = request_id
            return quote

    # ------------------------------------------------------------------
    # Requests, confirmed/executable allocations and concurrency control
    # ------------------------------------------------------------------

    def submit_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            requirement = self._normalize_requirement(payload)
            request_id = payload.get("id") or self._new_id("W", self.records["requests"])
            if any(r.get("id") == request_id for r in self.records["requests"]):
                raise ConflictError(f"request already exists: {request_id}")
            request = {
                "id": request_id,
                "company": requirement.get("company"),
                "site": requirement["site"],
                "medium": requirement["medium"],
                "flow_unit": requirement.get("flow_unit"),
                "total_unit": requirement.get("total_unit"),
                "range": requirement["range"],
                "start": iso(requirement["start"]),
                "end": iso(requirement["end"]),
                "status": "submitted",
                "submitted_at": iso(self.now()),
            }
            self.records["requests"].append(request)
            self._audit("request_submitted", request_id, payload={"site": request["site"]})
            result = copy.deepcopy(request)
            result["quote"] = self.quote_requirement(copy.deepcopy(request))
            return result

    def confirm_request(
        self,
        request_id: str,
        payload: dict[str, Any] | str | None = None,
        *,
        actor: Any | None = "library",
    ) -> dict[str, Any]:
        if isinstance(payload, str):
            payload = {"meter": payload}
        payload = payload or {}
        with self._lock:
            request = self.get_request(request_id)
            if request.get("allocation_id"):
                existing = self._find("allocations", request["allocation_id"])
                requested_meter = payload.get("meter") or payload.get("meter_id")
                if requested_meter and requested_meter != self._allocation_meter(existing):
                    raise ConflictError(
                        "request is already confirmed with another instrument",
                        details={"allocation": existing["id"]},
                    )
                result = self._public_allocation(existing)
                result["idempotent"] = True
                return result

            requirement = self._request_requirement(request)
            meter_id = payload.get("meter") or payload.get("meter_id")
            probe = payload.get("probe")
            if meter_id:
                candidate = self.assess_meter(meter_id, requirement, probe=probe)
            else:
                quote = self.quote_requirement(self._public_requirement(requirement))
                candidate = quote["recommended"]
                if candidate is None:
                    raise ConflictError(
                        "no instrument set satisfies this request",
                        details=quote["candidates"],
                    )

            if not candidate["eligible"]:
                raise ConflictError(
                    "request cannot become an executable rental because qualification failed",
                    details=candidate,
                )

            allocation_id = self._new_id("A-", self.records["allocations"])
            work_order_id = self._new_id("WO-", self.records["work_orders"])
            allocation = {
                "id": allocation_id,
                "request_id": request_id,
                "work_order_id": work_order_id,
                "company": request.get("company"),
                "site": requirement["site"],
                "medium": requirement["medium"],
                "flow_unit": requirement.get("flow_unit"),
                "total_unit": requirement.get("total_unit"),
                "range": request.get("range"),
                "start": iso(requirement["start"]),
                "end": iso(requirement["end"]),
                "status": "executable",
                "confirmed": True,
                "confirmed_at": iso(self.now()),
                "confirmed_by": actor_id(actor),
                "segments": [
                    {
                        "meter": candidate["meter"],
                        "probe": candidate["probe"],
                        "certificate_id": candidate["certificate"]["id"],
                        "from": iso(requirement["start"]),
                        "to": iso(requirement["end"]),
                    }
                ],
            }
            self.records["allocations"].append(allocation)
            work_order = {
                "id": work_order_id,
                "request_id": request_id,
                "allocation_id": allocation_id,
                "company": request.get("company"),
                "site": requirement["site"],
                "range": request.get("range"),
                "start": allocation["start"],
                "end": allocation["end"],
                "status": "ready",
                "segments": copy.deepcopy(allocation["segments"]),
            }
            self.records["work_orders"].append(work_order)
            request["status"] = "executable"
            request["allocation_id"] = allocation_id
            request["work_order_id"] = work_order_id
            self._audit(
                "request_confirmed",
                request_id,
                actor=actor_id(actor),
                payload={"allocation_id": allocation_id, "meter": candidate["meter"]},
            )
            result = self._public_allocation(allocation)
            result["work_order"] = work_order
            return result

    def _allocation_meter(self, allocation: dict[str, Any]) -> str | None:
        segments = allocation.get("segments") or []
        return segments[0].get("meter") if segments else allocation.get("meter")

    def _public_allocation(self, allocation: dict[str, Any]) -> dict[str, Any]:
        return copy.deepcopy(allocation)

    def meter_timeline(self, meter_id: str) -> dict[str, Any]:
        with self._lock:
            meter = self.get_meter(meter_id)
            return {
                "meter": copy.deepcopy(meter),
                "role": self.meter_role(meter_id),
                "certificates": [
                    copy.deepcopy(c)
                    for c in self.records.get("certificates", [])
                    if c.get("meter") == meter_id
                ],
                "certificate_revisions": [
                    copy.deepcopy(r)
                    for r in self.records.get("certificate_revision", [])
                    if self._find_certificate_for_revision(r).get("meter") == meter_id
                ],
                "probe_installations": [
                    copy.deepcopy(p)
                    for p in self.records.get("probe_installations", [])
                    if p.get("meter") == meter_id
                ],
                "allocations": [
                    copy.deepcopy(a)
                    for a in self.records.get("allocations", [])
                    if any(s.get("meter") == meter_id for s in a.get("segments", []))
                ],
                "return_cases": [
                    copy.deepcopy(c)
                    for c in self.records.get("return_cases", [])
                    if c.get("meter") == meter_id
                ],
                "handoffs": [
                    copy.deepcopy(h)
                    for h in self.records.get("handoffs", [])
                    if h.get("old_meter") == meter_id or h.get("new_meter") == meter_id
                ],
            }

    def _find_certificate_for_revision(self, revision: dict[str, Any]) -> dict[str, Any]:
        try:
            return self._find("certificates", revision.get("cert"))
        except NotFoundError:
            return {}

    def list_requests(self) -> list[dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self.records["requests"])

    def list_work_orders(self) -> list[dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self.records["work_orders"])

    def get_work_order(self, work_order_id: str) -> dict[str, Any]:
        with self._lock:
            if work_order_id.startswith("WO-"):
                work_order = self._find("work_orders", work_order_id)
            else:
                request = self.get_request(work_order_id)
                if not request.get("work_order_id"):
                    raise NotFoundError(f"work order not found for request {work_order_id}")
                work_order = self._find("work_orders", request["work_order_id"])
            result = copy.deepcopy(work_order)
            result["handoffs"] = [
                h
                for h in self.records.get("handoffs", [])
                if h.get("site") == work_order.get("site") and h.get("old_meter")
            ]
            result["recording_instructions"] = self._work_order_instructions(result)
            result["receipt_ids"] = [
                r["id"]
                for r in self.records.get("receipts", [])
                if r.get("agreement")
                and self.get_agreement(r["agreement"]).get("site") == work_order.get("site")
            ]
            return result

    def _work_order_instructions(self, work_order: dict[str, Any]) -> list[dict[str, Any]]:
        instructions = []
        req_range = work_order.get("range")
        start = parse_dt(work_order["start"])
        end = parse_dt(work_order["end"])
        for segment in work_order.get("segments", []):
            seg_start = parse_dt(segment["from"])
            seg_end = parse_dt(segment["to"])
            meter = segment["meter"]
            probe = segment.get("probe")
            cert_id = segment.get("certificate_id")
            revs = self._revision_hits(
                meter,
                probe,
                seg_start,
                seg_end,
                req_range or meter and self.get_meter(meter)["range"],
                cert_id,
            )
            instructions.append(
                {
                    "meter": meter,
                    "probe": probe,
                    "certificate_id": cert_id,
                    "interval": [segment["from"], segment["to"]],
                    "require_cut_at_interval_start_and_end": True,
                    "require_cut_at_each_handoff": True,
                    "revision_restrictions": revs,
                    "action": (
                        "stop_or_switch_before_affected_range_time"
                        if revs
                        else "record_continuously_with_verified_boundary_cuts"
                    ),
                }
            )
        return instructions

    # ------------------------------------------------------------------
    # Handoffs, returns and probe/certificate changes
    # ------------------------------------------------------------------

    def record_handoff(self, payload: dict[str, Any], *, actor: Any | None = "library") -> dict[str, Any]:
        with self._lock:
            site = payload.get("site")
            old_meter = payload.get("old_meter") or payload.get("old")
            new_meter = payload.get("new_meter") or payload.get("new")
            at_raw = payload.get("at") or payload.get("handoff_at")
            if not site or not old_meter or not new_meter or not at_raw:
                raise ValidationError("site, old_meter, new_meter and at are required")
            if old_meter == new_meter:
                raise ValidationError("old_meter and new_meter must differ")
            at = parse_dt(at_raw, "at")
            self.get_meter(old_meter)
            self.get_meter(new_meter)
            handoff = {
                "id": payload.get("id") or self._new_id("H", self.records.get("handoffs", [])),
                "site": site,
                "old_meter": old_meter,
                "new_meter": new_meter,
                "at": iso(at),
                "recorded_by": actor_id(actor),
            }

            allocation_id = payload.get("allocation_id")
            work_order_id = payload.get("work_order_id")
            allocation = None
            if allocation_id:
                allocation = self._find("allocations", allocation_id)
            elif work_order_id:
                wo = self._find("work_orders", work_order_id)
                allocation = self._find("allocations", wo["allocation_id"])
            else:
                for candidate in self.records["allocations"]:
                    if candidate.get("site") != site:
                        continue
                    for segment in candidate.get("segments", []):
                        if (
                            segment.get("meter") == old_meter
                            and parse_dt(segment["from"]) <= at < parse_dt(segment["to"])
                        ):
                            allocation = candidate
                            break
                    if allocation:
                        break

            target_segment = None
            replacement_end = None
            if allocation:
                for segment in allocation.get("segments", []):
                    if (
                        segment.get("meter") == old_meter
                        and parse_dt(segment["from"]) <= at < parse_dt(segment["to"])
                    ):
                        target_segment = segment
                        replacement_end = segment["to"]
                        break
                if target_segment is None:
                    raise ConflictError(
                        "old meter is not active in the linked allocation at the handoff instant"
                    )
                replacement_req = self._normalize_requirement(
                    {
                        "site": allocation.get("site", site),
                        "medium": allocation.get("medium") or payload.get("medium"),
                        "flow_unit": allocation.get("flow_unit"),
                        "total_unit": allocation.get("total_unit"),
                        "range": allocation.get("range") or payload.get("range"),
                        "start": iso(at),
                        "end": replacement_end,
                    }
                )
                candidate = self.assess_meter(
                    new_meter,
                    replacement_req,
                    probe=payload.get("new_probe"),
                    exclude_allocation_id=allocation["id"],
                )
                if not candidate["eligible"] and not payload.get("allow_unqualified"):
                    raise ConflictError(
                        "replacement instrument does not satisfy the remaining work order or is already confirmed",
                        details=candidate,
                    )
            elif payload.get("range"):
                replacement_req = self._normalize_requirement(
                    {
                        "site": site,
                        "medium": payload.get("medium"),
                        "flow_unit": payload.get("flow_unit"),
                        "total_unit": payload.get("total_unit"),
                        "range": payload.get("range"),
                        "start": iso(at),
                        "end": payload.get("until") or payload.get("end"),
                    }
                )
                candidate = self.assess_meter(
                    new_meter, replacement_req, probe=payload.get("new_probe")
                )
                if not candidate["eligible"] and not payload.get("allow_unqualified"):
                    raise ConflictError("replacement instrument is not qualified", details=candidate)

            self.records.setdefault("handoffs", []).append(handoff)

            if allocation and target_segment is not None:
                segment_index = allocation["segments"].index(target_segment)
                old_end = replacement_end
                target_segment["to"] = iso(at)
                target_segment["handoff_out"] = handoff["id"]
                new_meter_record = self.get_meter(new_meter)
                new_segment = {
                    "meter": new_meter,
                    "probe": payload.get("new_probe") or new_meter_record.get("probe"),
                    "certificate_id": self._make_segment_ref(
                        new_meter, at, parse_dt(old_end)
                    )["certificate_id"],
                    "from": iso(at),
                    "to": old_end,
                    "handoff_in": handoff["id"],
                }
                allocation["segments"].insert(segment_index + 1, new_segment)
                allocation["status"] = "temporarily_replaced"
                if allocation.get("work_order_id"):
                    wo = self._find("work_orders", allocation["work_order_id"])
                    wo["segments"] = copy.deepcopy(allocation["segments"])

            self._audit("handoff_recorded", handoff["id"], actor=actor_id(actor))
            return copy.deepcopy({"handoff": handoff, "allocation": allocation})

    def record_return(self, payload: dict[str, Any], *, actor: Any | None = "library") -> dict[str, Any]:
        with self._lock:
            meter_id = payload.get("meter")
            at_raw = payload.get("returned_at") or payload.get("at")
            if not meter_id or not at_raw:
                raise ValidationError("meter and returned_at are required")
            self.get_meter(meter_id)
            at = parse_dt(at_raw, "returned_at")
            case_id = payload.get("return_case_id") or self._new_id("RC-", self.records["return_cases"])
            if any(c["id"] == case_id for c in self.records["return_cases"]):
                raise ConflictError(f"return case already exists: {case_id}")
            case = {
                "id": case_id,
                "meter": meter_id,
                "return_event": {
                    "id": payload.get("return_event_id") or self._new_id("RR-", []),
                    "at": iso(at),
                    "source": payload.get("source") or "field_return_record",
                    "recorded_by": actor_id(actor),
                },
            }
            if payload.get("accepted_at"):
                accepted = parse_dt(payload["accepted_at"], "accepted_at")
                case["acceptance_event"] = {
                    "id": self._new_id("WA-", []),
                    "at": iso(accepted),
                    "source": payload.get("acceptance_source") or "warehouse_acceptance",
                    "recorded_by": actor_id(actor),
                }
            self.records["return_cases"].append(case)
            self._audit("return_recorded", case_id, actor=actor_id(actor))
            return copy.deepcopy(case)

    def accept_return(
        self,
        case_id: str,
        payload: dict[str, Any] | None = None,
        *,
        actor: Any | None = "library",
    ) -> dict[str, Any]:
        payload = payload or {}
        with self._lock:
            case = self._find("return_cases", case_id)
            if case.get("acceptance_event"):
                raise ConflictError("return case is already accepted")
            at = parse_dt(payload.get("accepted_at") or self.now().isoformat(), "accepted_at")
            case["acceptance_event"] = {
                "id": payload.get("acceptance_event_id") or f"WA-{case['id']}",
                "at": iso(at),
                "source": payload.get("source") or "warehouse_acceptance",
                "recorded_by": actor_id(actor),
            }
            self._audit("return_accepted", case_id, actor=actor_id(actor))
            return copy.deepcopy(case)

    def replace_probe(
        self,
        meter_id: str,
        new_probe: str,
        *,
        at: str | datetime | None = None,
        actor: Any | None = "library",
    ) -> dict[str, Any]:
        with self._lock:
            meter = self.get_meter(meter_id)
            if not new_probe:
                raise ValidationError("new_probe is required")
            when = parse_dt(at, "at") if at is not None else self.now()
            old_probe = meter.get("probe")
            event = {
                "id": self._new_id("PI-", self.records["probe_installations"]),
                "meter": meter_id,
                "old_probe": old_probe,
                "new_probe": new_probe,
                "at": iso(when),
                "recorded_by": actor_id(actor),
                "recertification_required": True,
            }
            self.records["probe_installations"].append(event)
            meter["probe"] = new_probe
            self._audit("probe_replaced", event["id"], actor=actor_id(actor))
            return copy.deepcopy(event)

    def add_certificate(self, payload: dict[str, Any], *, actor: Any | None = "verifier") -> dict[str, Any]:
        with self._lock:
            for field in ("id", "meter", "probe", "range", "from", "to"):
                if field not in payload:
                    raise ValidationError(f"certificate field required: {field}")
            parse_dt(payload["from"], "from")
            parse_dt(payload["to"], "to")
            if any(c["id"] == payload["id"] for c in self.records["certificates"]):
                raise ConflictError(f"certificate already exists: {payload['id']}")
            cert = copy.deepcopy(payload)
            self.records["certificates"].append(cert)
            self._audit("certificate_added", cert["id"], actor=actor_id(actor))
            return copy.deepcopy(cert)

    def add_revision(self, payload: dict[str, Any], *, actor: Any | None = "verifier") -> dict[str, Any]:
        with self._lock:
            for field in ("id", "cert", "probe", "affected_range", "effective_from", "effective_to"):
                if field not in payload:
                    raise ValidationError(f"revision field required: {field}")
            parse_dt(payload["effective_from"])
            parse_dt(payload["effective_to"])
            rev = copy.deepcopy(payload)
            rev.setdefault("reported_at", iso(self.now()))
            if any(r["id"] == rev["id"] for r in self.records["certificate_revision"]):
                raise ConflictError(f"revision already exists: {rev['id']}")
            self.records["certificate_revision"].append(rev)
            self._audit("certificate_revision_added", rev["id"], actor=actor_id(actor))
            return copy.deepcopy(rev)

    def _audit(self, event_type: str, subject: str, *, actor: str = "system", payload: Any = None) -> None:
        self.records["audit_events"].append(
            {
                "at": iso(self.now()),
                "type": event_type,
                "subject": subject,
                "actor": actor,
                "payload": payload,
            }
        )

    # ------------------------------------------------------------------
    # Measurement analysis
    # ------------------------------------------------------------------

    def _cut_map(self, sample: dict[str, Any]) -> dict[datetime, tuple[str, float]]:
        result = {}
        for raw_key, value in sample.get("cuts", {}).items():
            result[parse_dt(raw_key, "cut time")] = (raw_key, float(value))
        return result

    def analyze_receipt(self, receipt_id: str, actor: Any | None = None) -> dict[str, Any]:
        with self._lock:
            receipt = self.get_receipt(receipt_id, actor)
            agreement = self.get_agreement(receipt["agreement"])
            segments: list[dict[str, Any]] = []
            line_reports: list[dict[str, Any]] = []

            for line_index, line in enumerate(receipt.get("lines", []), start=1):
                sample = self._find("samples", line["sample"])
                line_start = parse_dt(line["from"], "line.from")
                line_end = parse_dt(line["to"], "line.to")
                cuts = self._cut_map(sample)
                in_interval = sorted(
                    [(dt, raw, value) for dt, (raw, value) in cuts.items() if line_start <= dt <= line_end],
                    key=lambda item: item[0],
                )
                has_boundaries = (
                    line_start in cuts and line_end in cuts and len(in_interval) >= 2
                )
                line_report = {
                    "line_index": line_index,
                    "sample_id": sample["id"],
                    "from": line["from"],
                    "to": line["to"],
                    "boundary_cuts_available": line_start in cuts and line_end in cuts,
                    "cuts_in_interval": [
                        {"at": raw, "reading": value} for _, raw, value in in_interval
                    ],
                }
                line_reports.append(line_report)

                if not has_boundaries:
                    segments.append(
                        {
                            "segment_id": f"{receipt_id}-L{line_index}-boundary",
                            "receipt_id": receipt_id,
                            "line_index": line_index,
                            "sample_id": sample["id"],
                            "meter": sample.get("meter"),
                            "probe": sample.get("probe"),
                            "site": sample.get("site"),
                            "medium": sample.get("medium"),
                            "start": line["from"],
                            "end": line["to"],
                            "original_amount": None,
                            "qualified": False,
                            "status": "missing_measured_boundary_cut",
                            "issues": ["only differences between measured boundary cuts are allowed"],
                            "reference_checks": [],
                        }
                    )
                    continue

                for cut_index in range(len(in_interval) - 1):
                    start_dt, start_raw, start_reading = in_interval[cut_index]
                    end_dt, end_raw, end_reading = in_interval[cut_index + 1]
                    segment = {
                        "segment_id": f"{receipt_id}-L{line_index}-S{cut_index + 1}",
                        "receipt_id": receipt_id,
                        "line_index": line_index,
                        "sample_id": sample["id"],
                        "meter": sample.get("meter"),
                        "probe": sample.get("probe"),
                        "site": sample.get("site"),
                        "medium": sample.get("medium"),
                        "start": iso(start_dt),
                        "end": iso(end_dt),
                        "start_reading": start_reading,
                        "end_reading": end_reading,
                        "original_amount": end_reading - start_reading,
                        "flow_range": sample.get("flow_range"),
                        "issues": [],
                        "correction_impacts": [],
                        "handoffs": [],
                        "reference_checks": [],
                    }
                    self._qualify_segment(
                        segment,
                        sample,
                        start_dt,
                        end_dt,
                        reference_allowed=bool(agreement.get("reference_allowed", False)),
                    )
                    segments.append(segment)

            self._mark_interval_overlaps(segments)
            totals = self._totals(segments)
            affected_revisions = sorted(
                {
                    impact["revision_id"]
                    for segment in segments
                    for impact in segment.get("correction_impacts", [])
                }
            )
            return {
                "receipt_id": receipt_id,
                "agreement_id": agreement["id"],
                "agreement": agreement,
                "line_reports": line_reports,
                "segments": segments,
                "totals": totals,
                "affected_revision_ids": affected_revisions,
                "responsible_parties": [
                    {"actor": agreement["issuer"], "role": "issuer_supply_party"},
                    {"actor": agreement["counterparty"], "role": "counterparty_demand_party"},
                ],
            }

    def _qualify_segment(
        self,
        segment: dict[str, Any],
        sample: dict[str, Any],
        start: datetime,
        end: datetime,
        *,
        reference_allowed: bool | None = None,
    ) -> None:
        # Handoff attribution.
        for handoff in self.records.get("handoffs", []):
            if not handoff.get("at") or handoff.get("site") != sample.get("site"):
                continue
            if handoff.get("old_meter") != sample.get("meter") and handoff.get("new_meter") != sample.get("meter"):
                continue
            at = parse_dt(handoff["at"])
            if start < at < end:
                segment["issues"].append("crosses_handoff_without_cut_at_handoff")
                segment["crossing_handoff_id"] = handoff["id"]
            elif at == start:
                segment["handoffs"].append(handoff["id"])
                if handoff.get("old_meter") == sample.get("meter"):
                    segment["issues"].append("old_meter_reading_interval_starts_after_handoff")
            elif at == end:
                segment["handoffs"].append(handoff["id"])
                if handoff.get("new_meter") == sample.get("meter"):
                    segment["issues"].append("new_meter_reading_interval_precedes_installation_handoff")

        # Exact host+probe certificate qualification.  Same model is irrelevant.
        cert = self._current_cert_for_interval(
            sample["meter"],
            sample.get("probe"),
            start,
            end,
            sample.get("flow_range"),
        )
        segment["certificate_id"] = cert["id"] if cert else None
        if cert is None:
            segment["issues"].append("no_applicable_certificate_for_exact_meter_probe_range_time")
        else:
            for rev in self.records.get("certificate_revision", []):
                revision_matches = rev.get("cert") == cert["id"] or (
                    rev.get("meter") == sample.get("meter")
                    and (not rev.get("probe") or rev.get("probe") == sample.get("probe"))
                )
                if not revision_matches:
                    continue
                rev_start = parse_dt(rev["effective_from"])
                rev_end = parse_dt(rev["effective_to"])
                if not dt_overlap(start, end, rev_start, rev_end):
                    continue
                if not ranges_overlap(rev["affected_range"], sample.get("flow_range") or rev["affected_range"]):
                    continue
                flow_lo, flow_hi = sample["flow_range"]
                aff_lo, aff_hi = rev["affected_range"]
                fully = flow_lo >= aff_lo and flow_hi <= aff_hi
                impact = {
                    "revision_id": rev["id"],
                    "certificate_id": cert["id"],
                    "affected_range": rev["affected_range"],
                    "effective_from": rev["effective_from"],
                    "effective_to": rev["effective_to"],
                    "relation": "fully_affected" if fully else "range_boundary_crossed_summary_cannot_apportion",
                }
                segment["correction_impacts"].append(impact)
                segment["issues"].append(
                    "certificate_revision_range_fully_affected"
                    if fully
                    else "certificate_revision_range_cannot_be_apportioned_from_min_max_summary"
                )

        segment["reference_checks"] = self._reference_checks(
            segment,
            sample,
            start,
            end,
            reference_allowed=reference_allowed,
        )
        valid_refs = [r for r in segment["reference_checks"] if r["valid"]]
        segment["selected_reference"] = None
        segment["reference_amount"] = None
        if len(valid_refs) == 1:
            selected = valid_refs[0]
            segment["selected_reference"] = selected["sample_id"]
            segment["reference_amount"] = selected["amount"]
            segment["reference_status"] = "valid_cover_requires_bilateral_review_confirmation"
        elif len(valid_refs) > 1:
            segment["selected_reference"] = None
            segment["reference_status"] = "multiple_references_must_choose_one_never_sum"
            segment["issues"].append("multiple_reference_sources_for_same_interval")
        elif any(r.get("interval_matched") for r in segment["reference_checks"]):
            segment["reference_status"] = "interval_matched_but_source_invalid"
        else:
            segment["reference_status"] = "no_matching_reference"

        segment["qualified"] = not segment["issues"]
        segment["status"] = "qualified" if segment["qualified"] else segment["issues"][0]

    def _reference_checks(
        self,
        segment: dict[str, Any],
        primary_sample: dict[str, Any],
        start: datetime,
        end: datetime,
        *,
        reference_allowed: bool | None = None,
    ) -> list[dict[str, Any]]:
        checks = []
        for ref in self.records.get("samples", []):
            if ref.get("reference") is not True:
                continue
            reasons: list[str] = []
            interval_matched = False
            amount = None

            if reference_allowed is False:
                reasons.append("agreement_does_not_allow_reference_record_type")
            if ref.get("meter") == primary_sample.get("meter") and ref.get("probe") == primary_sample.get("probe"):
                reasons.append("reference_not_independent_meter_probe")
            if ref.get("site") != primary_sample.get("site"):
                reasons.append("site_or_metering_point_mismatch")
            if ref.get("medium") != primary_sample.get("medium"):
                reasons.append("medium_mismatch")
            if primary_sample.get("flow_unit") and ref.get("flow_unit") != primary_sample.get("flow_unit"):
                reasons.append("flow_unit_mismatch")
            if primary_sample.get("total_unit") and ref.get("total_unit") != primary_sample.get("total_unit"):
                reasons.append("total_unit_mismatch")

            ref_cuts = self._cut_map(ref)
            if start in ref_cuts and end in ref_cuts:
                interval_matched = True
                amount = ref_cuts[end][1] - ref_cuts[start][1]
            else:
                reasons.append("reference_cut_boundaries_do_not_match_interval")

            cert = self._current_cert_for_interval(
                ref["meter"],
                ref.get("probe"),
                start,
                end,
                ref.get("flow_range"),
            )
            if cert is None:
                reasons.append("reference_has_no_independent_applicable_certificate")
            else:
                rev_hits = self._revision_hits(
                    ref["meter"], ref.get("probe"), start, end, ref.get("flow_range"), cert["id"]
                )
                if rev_hits:
                    reasons.append("reference_certificate_is_revised_for_use")

            checks.append(
                {
                    "sample_id": ref["id"],
                    "meter": ref["meter"],
                    "probe": ref.get("probe"),
                    "site": ref.get("site"),
                    "medium": ref.get("medium"),
                    "interval_matched": interval_matched,
                    "amount": amount,
                    "certificate_id": cert["id"] if cert else None,
                    "valid": not reasons,
                    "reasons": reasons,
                    "double_count_rule": "at most one reference amount may replace the primary amount",
                }
            )
        return checks

    def _mark_interval_overlaps(self, segments: list[dict[str, Any]]) -> None:
        ordered = sorted(
            [s for s in segments if s.get("original_amount") is not None],
            key=lambda s: parse_dt(s["start"]),
        )
        for previous, current in zip(ordered, ordered[1:]):
            if parse_dt(current["start"]) < parse_dt(previous["end"]):
                previous["overlap_conflict"] = True
                current["overlap_conflict"] = True
                current.setdefault("issues", []).append("measurement_interval_overlap_do_not_double_count")
                previous.setdefault("issues", []).append("measurement_interval_overlap_do_not_double_count")
                previous["qualified"] = False
                current["qualified"] = False

    def _totals(self, segments: list[dict[str, Any]]) -> dict[str, Any]:
        original = 0.0
        directly_citable = 0.0
        reference_proposed = 0.0
        reference_corroborated_original = 0.0
        review_original = 0.0
        for segment in segments:
            amount = segment.get("original_amount")
            if amount is None:
                continue
            original += amount
            if segment.get("overlap_conflict"):
                review_original += amount
                continue
            if segment.get("qualified"):
                directly_citable += amount
                reference_proposed += amount
            else:
                review_original += amount
                if segment.get("selected_reference") and segment.get("reference_amount") is not None:
                    reference_proposed += segment["reference_amount"]
                    reference_corroborated_original += amount
        return {
            "original_total": original,
            "directly_citable_total_before_review_confirmation": directly_citable,
            "reference_proposed_effective_total": reference_proposed,
            "reference_corroborated_original_effective_total": (
                directly_citable + reference_corroborated_original
            ),
            "review_required_original_total": review_original,
            "unit": next(
                (m.get("total_unit") for m in self.records["meters"] if segments and m["meter"] == segments[0]["meter"]),
                None,
            ),
        }

    # ------------------------------------------------------------------
    # Correction impact and review workflow
    # ------------------------------------------------------------------

    def correction_impact(self, revision_id: str) -> dict[str, Any]:
        with self._lock:
            rev = self._find("certificate_revision", revision_id)
            cert = self._find("certificates", rev["cert"])
            meter_id = cert["meter"]
            probe = rev.get("probe") or cert.get("probe")
            reported_at = parse_dt(rev["reported_at"], "reported_at")
            rev_start = parse_dt(rev["effective_from"])
            rev_end = parse_dt(rev["effective_to"])

            affected_requests = []
            for request in self.records.get("requests", []):
                req = self._request_requirement(request)
                time_overlap = dt_overlap(req["start"], req["end"], rev_start, rev_end)
                range_overlap = ranges_overlap(req["range"], rev["affected_range"])
                if time_overlap and range_overlap:
                    affected_requests.append(
                        {
                            "request_id": request["id"],
                            "work_order_id": request.get("work_order_id"),
                            "status": request.get("status"),
                            "time_overlap": time_overlap,
                            "range_overlap": range_overlap,
                            "recording_action": "stop_affected_range_or_switch_instrument",
                        }
                    )

            active_uses = []
            for allocation in self.records.get("allocations", []):
                for segment in allocation.get("segments", []):
                    if segment.get("meter") != meter_id or segment.get("probe") != probe:
                        continue
                    start = parse_dt(segment["from"])
                    end = parse_dt(segment["to"])
                    if start <= reported_at < end:
                        req_range = allocation.get("range") or self.get_meter(meter_id)["range"]
                        active_uses.append(
                            {
                                "allocation_id": allocation["id"],
                                "work_order_id": allocation.get("work_order_id"),
                                "site": allocation.get("site"),
                                "interval": [segment["from"], segment["to"]],
                                "range": req_range,
                                "range_overlap": ranges_overlap(req_range, rev["affected_range"]),
                            }
                        )

            stop_required = any(use["range_overlap"] for use in active_uses)
            location = self._meter_location_at(meter_id, reported_at)

            receipt_impacts = []
            review_cases = []
            for receipt in self.records.get("receipts", []):
                analysis = self.analyze_receipt(receipt["id"])
                affected_segments = [
                    s
                    for s in analysis["segments"]
                    if any(i["revision_id"] == revision_id for i in s.get("correction_impacts", []))
                ]
                if not affected_segments:
                    continue
                case_id = f"RV-{receipt['id']}-{revision_id}"
                review_cases.append(case_id)
                receipt_impacts.append(
                    {
                        "receipt_id": receipt["id"],
                        "agreement_id": receipt["agreement"],
                        "issued_at": receipt["issued_at"],
                        "issued_before_correction_reported": parse_dt(receipt["issued_at"]) < reported_at,
                        "affected_segment_ids": [s["segment_id"] for s in affected_segments],
                        "still_directly_citable_segment_ids": [
                            s["segment_id"] for s in analysis["segments"] if s.get("qualified")
                        ],
                        "reference_coverable_segment_ids": [
                            s["segment_id"]
                            for s in affected_segments
                            if s.get("selected_reference")
                        ],
                        "provisional_totals": analysis["totals"],
                        "responsible_parties": analysis["responsible_parties"],
                        "review_case_id": case_id,
                    }
                )

            return {
                "revision": copy.deepcopy(rev),
                "certificate": copy.deepcopy(cert),
                "meter": meter_id,
                "probe": probe,
                "reported_at": rev["reported_at"],
                "location_at_report": location,
                "active_uses_at_report": active_uses,
                "instrument_still_on_site": location.get("state") not in {"in_rental_library", "return_accepted"},
                "stop_required_now": stop_required,
                "stop_recording_work_orders_now": [use for use in active_uses if use["range_overlap"]],
                "current_usage_rule": (
                    "stop only an active use whose time and current range intersect the correction"
                ),
                "affected_requests_and_work_orders": affected_requests,
                "work_orders_requiring_range_recording_control": [
                    item.get("work_order_id") or item["request_id"] for item in affected_requests
                ],
                "affected_receipts": receipt_impacts,
                "review_case_ids": review_cases,
            }

    def _meter_location_at(self, meter_id: str, at: datetime) -> dict[str, Any]:
        for allocation in self.records.get("allocations", []):
            for segment in allocation.get("segments", []):
                if segment.get("meter") != meter_id:
                    continue
                if parse_dt(segment["from"]) <= at < parse_dt(segment["to"]):
                    return {
                        "state": "on_assignment",
                        "allocation_id": allocation["id"],
                        "work_order_id": allocation.get("work_order_id"),
                        "site": allocation.get("site"),
                    }
        latest_acceptance = None
        for case in self._custody_windows(meter_id):
            if case["accepted_at"]:
                accepted = parse_dt(case["accepted_at"])
                if accepted <= at and (latest_acceptance is None or accepted > latest_acceptance):
                    latest_acceptance = accepted
        if latest_acceptance:
            return {"state": "in_rental_library", "accepted_at": iso(latest_acceptance)}
        return {"state": "location_unknown_or_pending_return"}

    def receipt_impact(self, receipt_id: str, actor: Any | None = None) -> dict[str, Any]:
        with self._lock:
            receipt = self.get_receipt(receipt_id, actor)
            analysis = self.analyze_receipt(receipt_id, actor)
            corrections = []
            for rev_id in analysis["affected_revision_ids"]:
                rev = self._find("certificate_revision", rev_id)
                corrections.append(
                    {
                        "revision": copy.deepcopy(rev),
                        "impact": self.correction_impact(rev_id),
                    }
                )
            return {
                "receipt_id": receipt_id,
                "agreement_id": receipt["agreement"],
                "corrections": corrections,
                "responsible_parties": analysis["responsible_parties"],
                "review_case_ids": [f"RV-{receipt_id}-{rid}" for rid in analysis["affected_revision_ids"]],
                "totals": analysis["totals"],
                "segments": analysis["segments"],
            }

    def get_receipt_view(
        self, receipt_id: str, view: str = "summary", actor: Any | None = None
    ) -> dict[str, Any]:
        with self._lock:
            scope_name = "summary" if view not in {"original", "readings", "corrected"} else view
            receipt = self.get_receipt(receipt_id, actor, f"receipt:{scope_name}")
            if view == "original":
                return {
                    "view": "original",
                    "receipt_id": receipt_id,
                    "document": copy.deepcopy(receipt),
                    "integrity_note": "original issued receipt is never overwritten",
                }
            if view == "readings":
                analysis = self.analyze_receipt(receipt_id, "system")
                return {
                    "view": "readings",
                    "receipt_id": receipt_id,
                    "line_readings": analysis["line_reports"],
                    "segment_readings": [
                        {
                            "segment_id": s["segment_id"],
                            "sample_id": s["sample_id"],
                            "start": s["start"],
                            "end": s["end"],
                            "start_reading": s.get("start_reading"),
                            "end_reading": s.get("end_reading"),
                            "original_amount": s.get("original_amount"),
                        }
                        for s in analysis["segments"]
                    ],
                }
            if view == "corrected":
                confirmation_id = receipt.get("latest_confirmation_id")
                if confirmation_id and confirmation_id in self.confirmations:
                    return {
                        "view": "corrected",
                        "status": "confirmed",
                        "confirmation": copy.deepcopy(self.confirmations[confirmation_id]),
                    }
                analysis = self.analyze_receipt(receipt_id, "system")
                return {
                    "view": "corrected",
                    "status": "pending_bilateral_confirmation",
                    "provisional_totals": analysis["totals"],
                    "review_case_ids": [
                        f"RV-{receipt_id}-{rid}" for rid in analysis["affected_revision_ids"]
                    ],
                    "note": "provisional analysis is not a citable corrected voucher until both parties confirm",
                }
            if view == "summary":
                return self.receipt_impact(receipt_id, "system")
            raise ValidationError("view must be one of summary, original, readings, corrected")

    def issue_receipt(self, payload: dict[str, Any], *, actor: Any | None = None) -> dict[str, Any]:
        """Create an issued voucher from contract-selected sample intervals."""
        with self._lock:
            agreement_id = payload.get("agreement") or payload.get("agreement_id")
            if not agreement_id:
                raise ValidationError("agreement is required")
            agreement = self.get_agreement(agreement_id)
            who = actor_id(actor)
            if who not in {"system", "local-operator", agreement["issuer"]}:
                raise AuthorizationError("only the agreement issuer can issue this receipt")
            lines = payload.get("lines")
            if not isinstance(lines, list) or not lines:
                raise ValidationError("at least one selected line is required")
            normalized_lines = []
            for line in lines:
                sample_id = line.get("sample") or line.get("sample_id")
                sample = self._find("samples", sample_id)
                start = parse_dt(line.get("from"), "line.from")
                end = parse_dt(line.get("to"), "line.to")
                if start >= end:
                    raise ValidationError("receipt line from must be before to")
                # The selected endpoints must be actual verified cuts; no
                # time-averaging or invented boundary is permitted.
                cuts = self._cut_map(sample)
                if start not in cuts or end not in cuts:
                    raise ValidationError(
                        "selected receipt interval endpoints must exist as measured cuts",
                        details={"sample": sample_id},
                    )
                normalized_lines.append(
                    {"sample": sample_id, "from": iso(start), "to": iso(end)}
                )

            receipt_id = payload.get("id") or self._new_id("V", self.records["receipts"])
            if any(r["id"] == receipt_id for r in self.records["receipts"]):
                raise ConflictError(f"receipt already exists: {receipt_id}")
            issued_at = payload.get("issued_at") or iso(self.now())
            parse_dt(issued_at, "issued_at")
            receipt = {
                "id": receipt_id,
                "agreement": agreement_id,
                "issued_at": issued_at,
                "lines": normalized_lines,
            }
            self.records["receipts"].append(receipt)
            self._audit("receipt_issued", receipt_id, actor=who)
            return copy.deepcopy(receipt)

    def analyze_sample(self, sample_id: str) -> dict[str, Any]:
        with self._lock:
            sample = self._find("samples", sample_id)
            cuts = self._cut_map(sample)
            points = sorted(((dt, raw, value) for dt, (raw, value) in cuts.items()), key=lambda x: x[0])
            segments = []
            for index in range(len(points) - 1):
                start_dt, start_raw, start_reading = points[index]
                end_dt, end_raw, end_reading = points[index + 1]
                segment = {
                    "segment_id": f"{sample_id}-C{index + 1}",
                    "sample_id": sample_id,
                    "meter": sample.get("meter"),
                    "probe": sample.get("probe"),
                    "site": sample.get("site"),
                    "medium": sample.get("medium"),
                    "start": iso(start_dt),
                    "end": iso(end_dt),
                    "start_reading": start_reading,
                    "end_reading": end_reading,
                    "original_amount": end_reading - start_reading,
                    "flow_range": sample.get("flow_range"),
                    "issues": [],
                    "correction_impacts": [],
                    "handoffs": [],
                    "reference_checks": [],
                }
                self._qualify_segment(segment, sample, start_dt, end_dt)
                segments.append(segment)
            return {
                "sample": copy.deepcopy(sample),
                "segments": segments,
                "totals": self._totals(segments),
                "rule": "only adjacent verified cut differences are measurable; min/max flow cannot apportion volume",
            }

    def list_review_cases(self, actor: Any | None = None) -> list[dict[str, Any]]:
        with self._lock:
            cases = []
            who = actor_id(actor)
            for receipt in self.records["receipts"]:
                agreement = self.get_agreement(receipt["agreement"])
                is_party = who in {"system", "local-operator", agreement["issuer"], agreement["counterparty"]}
                analysis = self.analyze_receipt(receipt["id"], "system")
                for rev_id in analysis["affected_revision_ids"]:
                    case_id = f"RV-{receipt['id']}-{rev_id}"
                    state = self.review_states.get(case_id, {})
                    cases.append(
                        {
                            "id": case_id,
                            "receipt_id": receipt["id"],
                            "revision_id": rev_id,
                            "agreement_id": receipt["agreement"],
                            "status": state.get("status", "awaiting_bilateral_confirmation"),
                            "responsible_parties": analysis["responsible_parties"],
                            "confirmation_id": state.get("confirmation_id"),
                            "totals": analysis["totals"] if is_party else None,
                            "access": "full" if is_party else "routing_only",
                        }
                    )
            return cases

    def get_review_routing(self, case_id: str) -> dict[str, Any]:
        """Non-commercial routing view usable by the rental desk."""
        with self._lock:
            case = self.get_review_case(case_id, actor="system")
            state = self.review_states.get(case_id, {})
            return {
                "id": case_id,
                "view": "routing",
                "receipt_id": case["receipt_id"],
                "revision_id": case["revision_id"],
                "agreement_id": case["agreement_id"],
                "status": case["status"],
                "responsible_parties": case["responsible_parties"],
                "who_confirms": case["who_confirms"],
                "affected_segment_ids": case["affected_segment_ids"],
                "confirmation_id": state.get("confirmation_id"),
                "rental_library_role": "instrument_availability_only_not_voucher_confirmer",
                "links": {
                    "correction_impact": f"/v1/corrections/{case['revision_id']}/impact",
                    "party_review": f"/v1/reviews/{case_id}",
                },
            }

    def get_review_case(self, case_id: str, actor: Any | None = None) -> dict[str, Any]:
        with self._lock:
            if not case_id.startswith("RV-"):
                raise NotFoundError(f"review case not found: {case_id}")
            _, rest = case_id.split("RV-", 1)
            # Receipt and revision IDs contain hyphens too; deterministic suffix
            # is "-<revision id>" and receipt IDs are known.
            receipt_id = None
            revision_id = None
            for rev in self.records["certificate_revision"]:
                suffix = f"-{rev['id']}"
                if rest.endswith(suffix):
                    receipt_id = rest[: -len(suffix)]
                    revision_id = rev["id"]
                    break
            if receipt_id is None:
                raise NotFoundError(f"review case not found: {case_id}")
            receipt = self.get_receipt(receipt_id, actor)
            agreement = self.get_agreement(receipt["agreement"])
            analysis = self.analyze_receipt(receipt_id, actor)
            affected = [
                s
                for s in analysis["segments"]
                if any(i["revision_id"] == revision_id for i in s.get("correction_impacts", []))
            ]
            rev = self._find("certificate_revision", revision_id)
            state = self.review_states.setdefault(
                case_id,
                {
                    "id": case_id,
                    "status": "awaiting_proposal",
                    "proposal": None,
                    "party_confirmations": {},
                    "confirmation_id": None,
                },
            )
            default_decisions = self._default_decisions(analysis["segments"])
            case = {
                **copy.deepcopy(state),
                "id": case_id,
                "receipt_id": receipt_id,
                "revision_id": revision_id,
                "agreement_id": agreement["id"],
                "correction": copy.deepcopy(rev),
                "responsible_parties": analysis["responsible_parties"],
                "affected_segment_ids": [s["segment_id"] for s in affected],
                "analysis": analysis,
                "default_decisions": default_decisions,
                "who_confirms": [agreement["issuer"], agreement["counterparty"]],
                "rental_library_is_not_confirmation_party": True,
            }
            if actor_id(actor) not in {"system", "local-operator"}:
                self._require_review_party(case, actor)
            return case

    def _default_decisions(self, segments: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        decisions = {}
        for segment in segments:
            sid = segment["segment_id"]
            if segment.get("qualified") and segment.get("original_amount") is not None:
                decisions[sid] = {
                    "status": "effective",
                    "effective_amount": segment["original_amount"],
                    "basis": "original_cut_difference_with_applicable_certificate",
                }
            elif segment.get("selected_reference"):
                decisions[sid] = {
                    "status": "reference_supplemented",
                    "effective_amount": segment["reference_amount"],
                    "basis": "independent_parallel_reference_exact_source_and_interval",
                    "reference_id": segment["selected_reference"],
                }
            else:
                decisions[sid] = {
                    "status": "deferred_for_further_recheck",
                    "effective_amount": None,
                    "basis": "no_qualified_or_matching_reference_source",
                }
        return decisions

    def propose_review(
        self,
        case_id: str,
        payload: dict[str, Any] | str | None = None,
        *,
        actor: Any | None = None,
    ) -> dict[str, Any]:
        if isinstance(payload, str) and actor is None:
            actor = payload
            payload = {}
        payload = payload or {}
        with self._lock:
            case = self.get_review_case(case_id, actor)
            who, agreement = self._require_review_party(case, actor)
            if who == "system":
                # Direct system proposal is attributed to issuer unless an actor
                # is supplied.  HTTP always supplies/asserts an id.
                who = agreement["issuer"]
            decisions = self._validate_decisions(case, payload.get("decisions"))
            effective_total = self._decision_total(decisions)
            state = self.review_states[case_id]
            state["proposal"] = {
                "decisions": decisions,
                "effective_total": effective_total,
                "note": payload.get("note", ""),
                "proposed_by": who,
                "proposed_at": iso(self.now()),
            }
            state["party_confirmations"] = {
                who: {"at": iso(self.now()), "accepted_proposal_at": iso(self.now())}
            }
            state["status"] = "proposed_awaiting_other_party"
            self._audit("review_proposed", case_id, actor=who)
            return self.get_review_case(case_id, actor)

    def confirm_review(
        self,
        case_id: str,
        payload: dict[str, Any] | str | None = None,
        *,
        actor: Any | None = None,
    ) -> dict[str, Any]:
        if isinstance(payload, str) and actor is None:
            actor = payload
            payload = {}
        payload = payload or {}
        with self._lock:
            case = self.get_review_case(case_id, actor)
            who, agreement = self._require_review_party(case, actor)
            if who == "system":
                raise AuthorizationError("an explicit supply/demand party actor is required to confirm")
            state = self.review_states[case_id]
            if state.get("confirmation_id"):
                return copy.deepcopy(self.confirmations[state["confirmation_id"]])

            proposal = state.get("proposal")
            decisions = payload.get("decisions")
            if decisions is not None:
                validated = self._validate_decisions(case, decisions)
                if proposal and validated != proposal["decisions"]:
                    raise ConflictError(
                        "submitted decisions differ from the pending proposal; submit them as a revised proposal",
                        details={"pending_proposal": proposal},
                    )
                if not proposal:
                    effective_total = self._decision_total(validated)
                    proposal = state["proposal"] = {
                        "decisions": validated,
                        "effective_total": effective_total,
                        "note": payload.get("note", ""),
                        "proposed_by": who,
                        "proposed_at": iso(self.now()),
                    }
                    state["party_confirmations"] = {who: {"at": iso(self.now())}}
                    state["status"] = "proposed_awaiting_other_party"
                    return self.get_review_case(case_id, actor)
            elif not proposal:
                # Allow the second party to confirm the system default directly.
                decisions = case["default_decisions"]
                effective_total = self._decision_total(decisions)
                proposal = state["proposal"] = {
                    "decisions": decisions,
                    "effective_total": effective_total,
                    "note": payload.get("note", "default technical proposal"),
                    "proposed_by": "system-default",
                    "proposed_at": iso(self.now()),
                }
                state["party_confirmations"] = {}

            state["party_confirmations"][who] = {
                "at": iso(self.now()),
                "note": payload.get("note", ""),
            }
            parties = {agreement["issuer"], agreement["counterparty"]}
            confirmed = set(state["party_confirmations"].keys()) & parties
            if len(confirmed) < 2:
                state["status"] = "awaiting_other_party_confirmation"
                return self.get_review_case(case_id, actor)

            confirmation_id = f"CONF-{case_id[3:]}"  # CONF-<receipt>-<revision>
            analysis = case["analysis"]
            decisions = proposal["decisions"]
            confirmed_segments = []
            deferred_original = 0.0
            for segment in analysis["segments"]:
                decision = decisions.get(segment["segment_id"], {})
                item = {
                    "segment_id": segment["segment_id"],
                    "sample_id": segment["sample_id"],
                    "meter": segment["meter"],
                    "probe": segment["probe"],
                    "start": segment["start"],
                    "end": segment["end"],
                    "original_amount": segment.get("original_amount"),
                    "effective_amount": decision.get("effective_amount"),
                    "status": decision.get("status"),
                    "basis": decision.get("basis"),
                    "reference_id": decision.get("reference_id") or segment.get("selected_reference"),
                    "correction_impacts": segment.get("correction_impacts"),
                }
                confirmed_segments.append(item)
                if decision.get("effective_amount") is None and segment.get("original_amount") is not None:
                    deferred_original += segment["original_amount"]

            confirmation = {
                "id": confirmation_id,
                "citation": confirmation_id,
                "citable": True,
                "receipt_id": case["receipt_id"],
                "receipt_version": "corrected-v2",
                "review_case_id": case_id,
                "agreement_id": case["agreement_id"],
                "revision_id": case["revision_id"],
                "issued_at": iso(self.now()),
                "original_total": analysis["totals"]["original_total"],
                "effective_total": proposal["effective_total"],
                "deferred_original_total": deferred_original,
                "unit": analysis["totals"].get("unit"),
                "segments": confirmed_segments,
                "confirmed_by": [
                    {
                        "actor": agreement["issuer"],
                        "role": "issuer_supply_party",
                        "at": state["party_confirmations"].get(agreement["issuer"], {}).get("at"),
                    },
                    {
                        "actor": agreement["counterparty"],
                        "role": "counterparty_demand_party",
                        "at": state["party_confirmations"].get(agreement["counterparty"], {}).get("at"),
                    },
                ],
                "original_document_view": f"/v1/receipts/{case['receipt_id']}?view=original",
                "readings_view": f"/v1/receipts/{case['receipt_id']}?view=readings",
                "corrected_view": f"/v1/receipts/{case['receipt_id']}?view=corrected",
            }
            self.confirmations[confirmation_id] = confirmation
            self.records.setdefault("confirmations", []).append(copy.deepcopy(confirmation))
            state["status"] = "confirmed"
            state["confirmation_id"] = confirmation_id
            receipt = self.get_receipt(case["receipt_id"])
            receipt["latest_confirmation_id"] = confirmation_id
            self._audit("review_confirmed", case_id, payload={"confirmation_id": confirmation_id})
            return copy.deepcopy(confirmation)

    def _validate_decisions(
        self, case: dict[str, Any], raw_decisions: Any
    ) -> dict[str, dict[str, Any]]:
        if raw_decisions is None:
            raw_decisions = {}
        if not isinstance(raw_decisions, dict):
            raise ValidationError("decisions must be an object keyed by segment_id")
        valid_ids = {s["segment_id"]: s for s in case["analysis"]["segments"]}
        decisions = copy.deepcopy(case["default_decisions"])
        for sid, decision in raw_decisions.items():
            if sid not in valid_ids:
                raise ValidationError(f"unknown segment_id: {sid}")
            if not isinstance(decision, dict):
                raise ValidationError(f"decision for {sid} must be an object")
            amount = decision.get("effective_amount")
            if amount is not None:
                try:
                    amount = float(amount)
                except (TypeError, ValueError) as exc:
                    raise ValidationError(f"effective_amount for {sid} must be numeric or null") from exc
                if amount < 0:
                    raise ValidationError(f"effective_amount for {sid} cannot be negative")
            reference_id = decision.get("reference_id")
            if reference_id:
                segment = valid_ids[sid]
                valid_refs = {r["sample_id"]: r for r in segment.get("reference_checks", []) if r["valid"]}
                if reference_id not in valid_refs:
                    raise ValidationError(
                        f"reference {reference_id} is not a source/interval-matching valid reference for {sid}"
                    )
                if amount is None:
                    amount = valid_refs[reference_id]["amount"]
            decisions[sid] = {
                "status": decision.get("status")
                or ("effective" if amount is not None else "deferred_for_further_recheck"),
                "effective_amount": amount,
                "basis": decision.get("basis") or "bilaterally_agreed_recheck_result",
                "reference_id": reference_id,
            }
        effective = [
            (sid, valid_ids[sid])
            for sid, decision in decisions.items()
            if decision.get("effective_amount") is not None and sid in valid_ids
        ]
        for index, (sid_a, segment_a) in enumerate(effective):
            start_a = parse_dt(segment_a["start"])
            end_a = parse_dt(segment_a["end"])
            for sid_b, segment_b in effective[index + 1 :]:
                if dt_overlap(
                    start_a,
                    end_a,
                    parse_dt(segment_b["start"]),
                    parse_dt(segment_b["end"]),
                ):
                    raise ConflictError(
                        "overlapping effective measurement sources cannot both be counted",
                        details={"segments": [sid_a, sid_b], "rule": "do_not_double_count"},
                    )
        return decisions

    def _decision_total(self, decisions: dict[str, dict[str, Any]]) -> float:
        return float(
            sum(
                decision["effective_amount"]
                for decision in decisions.values()
                if decision.get("effective_amount") is not None
            )
        )

    def get_confirmation(self, confirmation_id: str, actor: Any | None = None) -> dict[str, Any]:
        with self._lock:
            if confirmation_id not in self.confirmations:
                raise NotFoundError(f"confirmation not found: {confirmation_id}")
            confirmation = self.confirmations[confirmation_id]
            self.get_receipt(confirmation["receipt_id"], actor, "receipt:corrected")
            return copy.deepcopy(confirmation)

    def actor_tasks(self, actor: str) -> dict[str, Any]:
        with self._lock:
            who = actor_id(actor)
            tasks = []
            for case_summary in self.list_review_cases(who):
                agreement = self.get_agreement(case_summary["agreement_id"])
                if who not in {agreement["issuer"], agreement["counterparty"]}:
                    continue
                state = self.review_states.get(case_summary["id"], {})
                confirmed = set(state.get("party_confirmations", {}).keys())
                if who not in confirmed or case_summary["status"] != "confirmed":
                    tasks.append(
                        {
                            "type": "voucher_recheck",
                            "review_case_id": case_summary["id"],
                            "receipt_id": case_summary["receipt_id"],
                            "revision_id": case_summary["revision_id"],
                            "agreement_id": agreement["id"],
                            "your_role": "issuer" if who == agreement["issuer"] else "counterparty",
                            "status": case_summary["status"],
                        }
                    )
            return {"actor": who, "tasks": tasks}

    # ------------------------------------------------------------------
    # End-to-end local scenario report
    # ------------------------------------------------------------------

    def scenario_report(self) -> dict[str, Any]:
        with self._lock:
            w2 = self.recommend_for_request("W2")
            impact = self.correction_impact("F17")
            receipt_impact = self.receipt_impact("V1")
            case = self.get_review_case("RV-V1-F17")
            return {
                "new_work_order_W2_recommendation": w2["recommended_instrument_set"],
                "W2_candidates": [
                    {
                        "meter": c["meter"],
                        "probe": c["probe"],
                        "eligible": c["eligible"],
                        "reasons": c["reasons"],
                        "actual_available_range": c["actual_available_range"],
                        "delivery_window": c["delivery_window"],
                    }
                    for c in w2["candidates"]
                ],
                "correction_F17": {
                    "stop_required_now": impact["stop_required_now"],
                    "location_at_report": impact["location_at_report"],
                    "affected_work_orders": impact["affected_requests_and_work_orders"],
                    "affected_receipts": impact["affected_receipts"],
                },
                "receipt_V1_totals": receipt_impact["totals"],
                "review_case": {
                    "id": case["id"],
                    "who_confirms": case["who_confirms"],
                    "default_decisions": case["default_decisions"],
                    "segments": [
                        {
                            "segment_id": s["segment_id"],
                            "interval": [s["start"], s["end"]],
                            "original_amount": s["original_amount"],
                            "status": s["status"],
                            "selected_reference": s.get("selected_reference"),
                            "reference_amount": s.get("reference_amount"),
                            "reference_checks": [
                                {
                                    "sample_id": r["sample_id"],
                                    "valid": r["valid"],
                                    "amount": r["amount"],
                                    "reasons": r["reasons"],
                                }
                                for r in s.get("reference_checks", [])
                            ],
                        }
                        for s in case["analysis"]["segments"]
                    ],
                },
            }
