import json
import unittest
from datetime import datetime
from pathlib import Path


class SourceRecordsTest(unittest.TestCase):
    def setUp(self):
        path = Path(__file__).resolve().parents[1] / "data" / "source_records.json"
        self.data = json.loads(path.read_text(encoding="utf-8"))

    def instant(self, value):
        instant = datetime.fromisoformat(value)
        self.assertIsNotNone(instant.utcoffset())
        return instant

    def test_source_documents_agree(self):
        cred = self.data["certificate_revision"][0]
        s1, s2, r1, r2 = self.data["samples"]
        change = self.data["handoffs"][0]["at"]
        self.assertIn(change, s1["cuts"])
        self.assertIn(change, s2["cuts"])
        self.assertEqual(r1["site"], s1["site"])
        self.assertNotEqual(r2["site"], s1["site"])
        self.assertLess(max(r1["cuts"]), change)
        self.assertLess(self.instant(self.data["receipts"][0]["issued_at"]), self.instant(cred["reported_at"]))
        self.assertLess(self.data["requests"][1]["range"][1], cred["affected_range"][0])

