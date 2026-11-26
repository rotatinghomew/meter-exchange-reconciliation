import json
import sys
import threading
import unittest
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from meter_service import (  # noqa: E402
    AuthorizationError,
    ConflictError,
    MeterService,
    build_server,
)


class MeterQualificationTest(unittest.TestCase):
    def setUp(self):
        self.service = MeterService()

    def test_new_w2_recommends_returned_and_accepted_fm18(self):
        quote = self.service.recommend_for_request("W2")
        recommended = quote["recommended_instrument_set"]
        self.assertEqual(recommended["meter"], "FM18")
        self.assertEqual(recommended["probe"], "B89")
        self.assertEqual(recommended["certificate_id"], "C51")
        self.assertEqual(recommended["actual_available_range"], [5, 150])
        self.assertEqual(
            recommended["delivery_window"]["next_full_duration_window"],
            [
                "2026-11-28T10:00:00+08:00",
                "2026-11-28T18:00:00+08:00",
            ],
        )
        fm18 = next(c for c in quote["candidates"] if c["meter"] == "FM18")
        self.assertTrue(fm18["eligible"])

    def test_exact_host_probe_certificate_is_required(self):
        req = {
            "site": "line-b",
            "medium": "food-water",
            "range": [5, 60],
            "start": "2026-11-28T10:00:00+08:00",
            "end": "2026-11-28T18:00:00+08:00",
        }
        candidate = self.service.assess_meter("FM19", self.service._normalize_requirement(req), probe="B90")
        self.assertTrue(candidate["eligible"])

        event = self.service.replace_probe("FM19", "B99", at="2026-11-28T09:00:00+08:00")
        self.assertTrue(event["recertification_required"])
        changed = self.service.assess_meter("FM19", self.service._normalize_requirement(req))
        self.assertFalse(changed["eligible"])
        self.assertIn("no_certificate_for_exact_meter_probe_combination", changed["reasons"])

        # C52 belongs to FM19/B90; an FM18 certificate or same model cannot cover B99.
        self.service.add_certificate(
            {
                "id": "C99",
                "meter": "FM19",
                "probe": "B99",
                "range": [5, 180],
                "from": "2026-10-01T00:00:00+08:00",
                "to": "2027-01-01T00:00:00+08:00",
            }
        )
        recertified = self.service.assess_meter("FM19", self.service._normalize_requirement(req))
        self.assertTrue(recertified["eligible"])
        self.assertEqual(recertified["certificate"]["id"], "C99")

    def test_partial_range_can_remain_outside_correction(self):
        req = self.service._normalize_requirement(
            {
                "site": "line-a",
                "medium": "food-water",
                "range": [5, 60],
                "start": "2026-11-27T11:05:00+08:00",
                "end": "2026-11-27T11:14:00+08:00",
            }
        )
        candidate = self.service.assess_meter("FM18", req)
        self.assertTrue(candidate["eligible"], candidate["reasons"])
        self.assertEqual(candidate["effective_usable_ranges"], [[5, 70]])

        affected_req = self.service._normalize_requirement(
            {
                "site": "line-a",
                "medium": "food-water",
                "range": [30, 120],
                "start": "2026-11-27T11:05:00+08:00",
                "end": "2026-11-27T11:14:00+08:00",
            }
        )
        affected = self.service.assess_meter("FM18", affected_req)
        self.assertFalse(affected["eligible"])
        self.assertIn("certificate_revision_affects_requested_use", affected["reasons"])

    def test_return_and_acceptance_gap_blocks_next_rental(self):
        req = self.service._normalize_requirement(
            {
                "site": "line-b",
                "medium": "food-water",
                "range": [5, 60],
                "start": "2026-11-27T11:30:00+08:00",
                "end": "2026-11-27T12:30:00+08:00",
            }
        )
        candidate = self.service.assess_meter("FM18", req)
        self.assertFalse(candidate["eligible"])
        self.assertIn("return_acceptance_source_gap", candidate["reasons"])
        self.assertEqual(
            candidate["delivery_window"]["next_full_duration_window"][0],
            "2026-11-27T12:00:00+08:00",
        )

    def test_concurrent_confirmations_cannot_take_same_confirmed_meter(self):
        n = 16
        request_ids = []
        for i in range(n):
            request = self.service.submit_request(
                {
                    "company": f"plant-{i}",
                    "site": "line-b",
                    "medium": "food-water",
                    "range": [5, 60],
                    "start": "2026-12-01T10:00:00+08:00",
                    "end": "2026-12-01T12:00:00+08:00",
                }
            )
            request_ids.append(request["id"])

        barrier = threading.Barrier(n)

        def confirm(request_id):
            barrier.wait()
            return self.service.confirm_request(request_id, {"meter": "FM18"})

        successes = 0
        conflicts = 0
        with ThreadPoolExecutor(max_workers=n) as pool:
            futures = [pool.submit(confirm, rid) for rid in request_ids]
            for future in futures:
                try:
                    future.result(timeout=5)
                    successes += 1
                except ConflictError:
                    conflicts += 1
        self.assertEqual(successes, 1)
        self.assertEqual(conflicts, n - 1)


class HandoffAndEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.service = MeterService()

    def test_work_order_preserves_old_new_handoff_chain(self):
        wo = self.service.get_work_order("W1")
        meters = [(s["meter"], s["from"], s["to"]) for s in wo["segments"]]
        self.assertEqual(meters[0][0], "FM18")
        self.assertEqual(meters[0][2], "2026-11-27T11:00:00+08:00")
        self.assertEqual(meters[1][0], "FM19")
        self.assertEqual(meters[1][1], "2026-11-27T11:00:00+08:00")
        self.assertEqual(wo["segments"][0]["handoff_out"], "H1")
        self.assertEqual(wo["segments"][1]["handoff_in"], "H1")

    def test_dynamic_handoff_blocks_replacement_meter_for_other_rentals(self):
        request = self.service.submit_request(
            {
                "company": "plant-c",
                "site": "line-c",
                "medium": "food-water",
                "range": [5, 60],
                "start": "2026-12-03T10:00:00+08:00",
                "end": "2026-12-03T12:00:00+08:00",
            }
        )
        allocation = self.service.confirm_request(request["id"], {"meter": "FM18"})
        wo_id = allocation["work_order_id"]
        result = self.service.record_handoff(
            {
                "work_order_id": wo_id,
                "site": "line-c",
                "old_meter": "FM18",
                "new_meter": "FM19",
                "at": "2026-12-03T11:00:00+08:00",
            }
        )
        meters = [s["meter"] for s in result["allocation"]["segments"]]
        self.assertEqual(meters, ["FM18", "FM19"])

        overlapping = self.service._normalize_requirement(
            {
                "site": "line-d",
                "medium": "food-water",
                "range": [5, 60],
                "start": "2026-12-03T11:30:00+08:00",
                "end": "2026-12-03T12:30:00+08:00",
            }
        )
        candidate = self.service.assess_meter("FM19", overlapping)
        self.assertFalse(candidate["eligible"])
        self.assertIn("confirmed_allocation_conflict", candidate["reasons"])

    def test_receipt_keeps_cut_segments_and_does_not_hard_combine_meters(self):
        analysis = self.service.analyze_receipt("V1", actor="plant-a")
        amounts = [s["original_amount"] for s in analysis["segments"]]
        self.assertEqual(amounts, [870.0, 1650.0, 1700.0])
        self.assertEqual(analysis["totals"]["original_total"], 4220.0)
        self.assertEqual(analysis["totals"]["directly_citable_total_before_review_confirmation"], 1700.0)

        by_id = {s["segment_id"]: s for s in analysis["segments"]}
        first = by_id["V1-L1-S1"]
        self.assertEqual(first["selected_reference"], "R7")
        self.assertEqual(first["reference_amount"], 860.0)
        second = by_id["V1-L1-S2"]
        r8 = next(r for r in second["reference_checks"] if r["sample_id"] == "R8")
        self.assertFalse(r8["valid"])
        self.assertIn("site_or_metering_point_mismatch", r8["reasons"])
        self.assertEqual(r8["amount"], 1560.0)
        self.assertIsNone(second["selected_reference"])

        raw_s18 = self.service.analyze_sample("S18")
        self.assertIn(
            "old_meter_reading_interval_starts_after_handoff",
            raw_s18["segments"][2]["issues"],
        )

    def test_correction_impact_distinguishes_historical_voucher_and_current_site(self):
        impact = self.service.correction_impact("F17")
        self.assertFalse(impact["stop_required_now"])
        self.assertEqual(impact["location_at_report"]["state"], "in_rental_library")
        self.assertEqual(impact["affected_receipts"][0]["receipt_id"], "V1")
        self.assertEqual(
            impact["affected_receipts"][0]["responsible_parties"],
            [
                {"actor": "plant-a", "role": "issuer_supply_party"},
                {"actor": "buyer-b", "role": "counterparty_demand_party"},
            ],
        )

    def test_bilateral_confirmation_creates_separately_citable_corrected_voucher(self):
        service = self.service
        service.propose_review("RV-V1-F17", actor="plant-a")
        case = service.confirm_review("RV-V1-F17", actor="buyer-b")
        self.assertEqual(case["id"], "CONF-V1-F17")
        self.assertTrue(case["citable"])
        self.assertEqual(case["original_total"], 4220.0)
        self.assertEqual(case["effective_total"], 2560.0)
        self.assertEqual(case["deferred_original_total"], 1650.0)

        original = service.get_receipt_view("V1", "original", actor="plant-a")
        self.assertEqual(original["document"]["lines"][0]["sample"], "S18")
        readings = service.get_receipt_view("V1", "readings", actor="buyer-b")
        self.assertEqual(readings["segment_readings"][0]["start_reading"], 350)
        self.assertEqual(readings["segment_readings"][0]["end_reading"], 1220)
        corrected = service.get_receipt_view("V1", "corrected", actor="plant-a")
        self.assertEqual(corrected["status"], "confirmed")
        self.assertEqual(corrected["confirmation"]["effective_total"], 2560.0)

    def test_parties_may_agree_original_reference_checked_amount(self):
        decisions = {
            "V1-L1-S1": {
                "effective_amount": 870,
                "basis": "parties_accept_original_cut_after_R7_source_interval_check",
                "reference_id": "R7",
            }
        }
        self.service.propose_review(
            "RV-V1-F17", {"decisions": decisions, "note": "difference within agreed procedure"}, actor="plant-a"
        )
        confirmation = self.service.confirm_review("RV-V1-F17", actor="buyer-b")
        self.assertEqual(confirmation["effective_total"], 2570.0)

    def test_only_agreement_parties_can_read_or_confirm_past_voucher(self):
        with self.assertRaises(AuthorizationError):
            self.service.get_receipt_view("V1", "original", actor="library")
        with self.assertRaises(AuthorizationError):
            self.service.propose_review("RV-V1-F17", actor="library")
        routing = self.service.get_review_routing("RV-V1-F17")
        self.assertEqual(routing["who_confirms"], ["plant-a", "buyer-b"])
        library_cases = self.service.list_review_cases("library")
        self.assertEqual(library_cases[0]["access"], "routing_only")
        self.assertIsNone(library_cases[0]["totals"])
        self.service.propose_review("RV-V1-F17", actor="plant-a")
        # Same party cannot manufacture bilateral confirmation.
        case = self.service.confirm_review("RV-V1-F17", actor="plant-a")
        self.assertEqual(case["status"], "awaiting_other_party_confirmation")

    def test_returning_and_reallocating_meter_does_not_revoke_old_review(self):
        self.service.propose_review("RV-V1-F17", actor="plant-a")
        confirmation = self.service.confirm_review("RV-V1-F17", actor="buyer-b")
        allocation = self.service.confirm_request("W2", {"meter": "FM18"}, actor="library")
        self.assertEqual(allocation["status"], "executable")

        # A later warehouse event for FM18 does not cascade to V1 or W1.
        rc = self.service.record_return(
            {"meter": "FM18", "returned_at": "2026-12-01T12:05:00+08:00"},
            actor="field-source",
        )
        self.service.accept_return(
            rc["id"], {"accepted_at": "2026-12-01T12:30:00+08:00"}, actor="warehouse"
        )
        stored = self.service.get_confirmation(confirmation["id"], actor="buyer-b")
        self.assertEqual(stored["effective_total"], 2560.0)
        self.assertTrue(self.service.get_work_order("W1"))
        self.assertTrue(self.service.get_request("W2")["allocation_id"])


class HttpApiTest(unittest.TestCase):
    server: ThreadingHTTPServer

    @classmethod
    def setUpClass(cls):
        cls.service = MeterService()
        cls.server = build_server(cls.service, host="127.0.0.1", port=0)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def request(self, method, path, body=None, actor=None):
        data = None
        headers = {"Content-Type": "application/json"}
        if actor:
            headers["X-Actor-Id"] = actor
        if body is not None:
            data = json.dumps(body).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                return response.status, json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode())

    def test_health_quote_and_review_http_flow(self):
        status, health = self.request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertTrue(health["ok"])

        status, quote = self.request("GET", "/v1/requests/W2/recommendation")
        self.assertEqual(status, 200)
        self.assertEqual(quote["recommended_instrument_set"]["meter"], "FM18")

        status, availability = self.request(
            "GET",
            "/v1/availability?site=line-b&medium=food-water&lo=5&hi=60"
            "&start=2026-11-28T10:00:00%2B08:00&end=2026-11-28T18:00:00%2B08:00",
        )
        self.assertEqual(status, 200)
        self.assertEqual(availability["recommended_instrument_set"]["meter"], "FM18")

        status, case = self.request(
            "POST", "/v1/reviews/RV-V1-F17/propose", {}, actor="plant-a"
        )
        self.assertEqual(status, 200)
        self.assertEqual(case["status"], "proposed_awaiting_other_party")

        status, confirmation = self.request(
            "POST", "/v1/reviews/RV-V1-F17/confirm", {}, actor="buyer-b"
        )
        self.assertEqual(status, 200)
        self.assertEqual(confirmation["effective_total"], 2560.0)

        status, denied = self.request("GET", "/v1/receipts/V1?view=original", actor="library")
        self.assertEqual(status, 403)
        self.assertEqual(denied["error"], "authorization_error")

        status, allowed = self.request("GET", "/v1/receipts/V1?view=corrected", actor="plant-a")
        self.assertEqual(status, 200)
        self.assertEqual(allowed["confirmation"]["id"], "CONF-V1-F17")


if __name__ == "__main__":
    unittest.main()
