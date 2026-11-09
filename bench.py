#!/usr/bin/env python3
"""ClickHouse 26.10 review: dataset generation, SQL, setup, checks, timing, reporting.

Commands:
  selftest                      offline checks, no containers, no timings
  generate DEST [options]       write one verified synthetic fixture
  plan                          print the balanced engine schedule
  run --rows N [options]        full four-engine run (needs podman and sudo)
  report                        rebuild results.txt from results.csv
"""
import argparse
import csv
import hashlib
import http.client
import json
import math
import os
import platform
import random
import re
import shutil
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
GENERATOR_VERSION = "0.3.0"

# ---------------------------------------------------------------- data

TESTS = ("A1", "A2", "S1", "S2", "T1", "J1", "C1")
COUNTRIES = ("au", "br", "de", "gb", "in", "jp", "us")
START = datetime(2026, 9, 23)
LOWER = "2026-10-16 00:00:00"
UPPER = "2026-10-23 00:00:00"


@dataclass(frozen=True)
class Scenario:
    rows: int = 100000
    customers: int = 1000
    selectivity_ppm: int = 10000
    seed: int = 1177
    skew: bool = False
    text_profile: str = "mixed"

    def validate(self):
        if self.text_profile not in ("mixed", "random", "repetitive"):
            raise ValueError("Unknown text profile")
        if not 1 <= self.customers <= self.rows:
            raise ValueError("Require 1 <= customers <= rows")
        if self.rows > 1000000000:
            raise ValueError("Maximum supported fixture size is 1 billion rows")
        if not 0 <= self.selectivity_ppm <= 500000:
            raise ValueError("Selectivity must be 0..500000 ppm (disjoint markers)")


def permutation(n, seed):
    rng = random.Random(seed)
    a = rng.randrange(1, n + 1)
    while math.gcd(a, n) != 1:
        a = a % n + 1
    return a, rng.randrange(n)


def order_rows(s):
    s.validate()
    # Separate RNG streams keep text selectivity independent of key and time factors.
    rng = random.Random(s.seed + 1)
    skew_rng = random.Random(s.seed + 3)
    text_rng = random.Random(s.seed + 4)
    a, b = permutation(s.rows, s.seed + 2)
    matched = s.rows * s.selectivity_ppm // 1000000
    for i in range(s.rows):
        customer = i % s.customers + 1
        if s.skew and i >= s.customers and skew_rng.random() < 0.8:
            customer = 1
        amount = rng.randrange(1, 1000001)
        status = ("paid", "shipped", "cancelled")[rng.randrange(3)]
        ts = START + timedelta(seconds=rng.randrange(30 * 86400))
        # Mandatory inclusive and exclusive boundary fixtures for T1.
        if i < 4:
            ts = (datetime(2026, 10, 15, 23, 59, 59), datetime(2026, 10, 16),
                  datetime(2026, 10, 22, 23, 59, 59), datetime(2026, 10, 23))[i]
        rank = (a * i + b) % s.rows
        # Separate alphabets guarantee marker words cannot occur accidentally.
        if s.text_profile == "repetitive":
            message = "x" * 128
        elif s.text_profile == "random":
            message = "".join(text_rng.choices("abcdfghijkmopqrstuvwxyz0123456789", k=128))
        else:
            message = (f"service api route orders status {status} request " +
                       "".join(text_rng.choices("0123456789abcdef", k=80))).ljust(128, " ")[:128]
        # Equal match counts for the separate prefix and substring markers.
        if rank < matched:
            message = "needle" + message[6:]
        elif rank < 2 * matched:
            message = message[:61] + "search" + message[67:]
        yield (i + 1, customer, amount, status, ts.strftime("%Y-%m-%d %H:%M:%S"), message)


def customer_rows(s):
    return ((i, COUNTRIES[(i - 1) % len(COUNTRIES)]) for i in range(1, s.customers + 1))


def read_customers(path):
    customers = {}
    with Path(path).open(newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            if len(row) != 2 or int(row[0]) <= 0 or int(row[0]) in customers:
                raise ValueError("Invalid or duplicate customer ID")
            customers[int(row[0])] = row[1]
    return customers


def oracle(rows, customers):
    groups = defaultdict(lambda: [0, 0])
    window = defaultdict(lambda: [0, 0])
    countries = defaultdict(lambda: [0, 0])
    keys = set()
    prefix_n = substring_n = prefix_sum = substring_sum = total = 0
    for _, customer, cents, status, ts, message in rows:
        keys.add(customer)
        groups[customer][0] += 1
        groups[customer][1] += cents
        total += cents
        if message.startswith("needle"):
            prefix_n += 1
            prefix_sum += cents
        if "search" in message:
            substring_n += 1
            substring_sum += cents
        if LOWER <= ts < UPPER:
            window[status][0] += 1
            window[status][1] += cents
        if customer not in customers:
            raise ValueError("Unresolved customer ID")
        country = customers[customer]
        countries[country][0] += 1
        countries[country][1] += cents
    pack = lambda d: [[k, *v] for k, v in sorted(d.items())]
    return {"A1": pack(groups), "A2": [[len(keys)]], "S1": [[prefix_n, prefix_sum]],
            "S2": [[substring_n, substring_sum]], "T1": pack(window),
            "J1": pack(countries), "C1": [[total]]}


def canonical_hash(rows):
    # Integers are never cast to float: values above 2**53 must stay distinct.
    return hashlib.sha256(json.dumps(rows, ensure_ascii=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1048576), b""):
            h.update(block)
    return h.hexdigest()


def read_orders(path):
    with Path(path).open(newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            yield (int(row[0]), int(row[1]), int(row[2]), *row[3:])


def generate(destination, s):
    s.validate()
    dest = Path(destination)
    # Refuse accidental replacement of existing evidence.
    dest.mkdir(parents=True, exist_ok=False)
    for filename, rows in [("customers.csv", customer_rows(s)), ("orders.csv", order_rows(s))]:
        with (dest / filename).open("w", newline="", encoding="utf-8") as f:
            csv.writer(f, lineterminator="\n").writerows(rows)
    expected = oracle(read_orders(dest / "orders.csv"), read_customers(dest / "customers.csv"))
    (dest / "expected.json").write_text(json.dumps(expected, indent=1) + "\n")
    manifest = {"format_version": 2, "generator_version": GENERATOR_VERSION, "scenario": asdict(s),
                "timestamps": "UTC, whole seconds; CSV has no timezone suffix",
                "window": {"lower_inclusive": LOWER, "upper_exclusive": UPPER},
                "prefix_matches": expected["S1"][0][0], "substring_matches": expected["S2"][0][0],
                "distinct_customers": expected["A2"][0][0],
                "files": {n: sha256(dest / n) for n in ("customers.csv", "orders.csv", "expected.json")},
                "answer_hashes": {k: canonical_hash(v) for k, v in expected.items()}}
    (dest / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def verify(destination):
    dest = Path(destination)
    manifest = json.loads((dest / "manifest.json").read_text())
    if manifest.get("format_version") != 2:
        raise ValueError("Unsupported fixture version; regenerate")
    scenario = Scenario(**manifest["scenario"])
    scenario.validate()
    if set(manifest["files"]) != {"customers.csv", "orders.csv", "expected.json"}:
        raise ValueError("Unexpected manifest file set")
    for filename, digest in manifest["files"].items():
        if sha256(dest / filename) != digest:
            raise ValueError("File hash mismatch: " + filename)
    expected = json.loads((dest / "expected.json").read_text())
    if expected != oracle(read_orders(dest / "orders.csv"), read_customers(dest / "customers.csv")):
        raise ValueError("Expected answers disagree with fixture")
    if {k: canonical_hash(v) for k, v in expected.items()} != manifest["answer_hashes"]:
        raise ValueError("Answer hashes disagree")
    customers = read_customers(dest / "customers.csv")
    count = 0
    for count, row in enumerate(read_orders(dest / "orders.csv"), 1):
        if len(row) != 6 or row[0] != count or len(row[5]) != 128 or not row[5].isascii():
            raise ValueError("Invalid order shape, ID or message")
    target = scenario.rows * scenario.selectivity_ppm // 1000000
    facts = {"prefix_matches": expected["S1"][0][0], "substring_matches": expected["S2"][0][0],
             "distinct_customers": expected["A2"][0][0]}
    if (count != scenario.rows or len(customers) != scenario.customers or
            facts["distinct_customers"] != scenario.customers or
            facts["prefix_matches"] != target or facts["substring_matches"] != target or
            any(manifest.get(k) != v for k, v in facts.items())):
        raise ValueError("Scenario metadata disagrees with actual fixture")
    return manifest, expected


# ---------------------------------------------------------------- queries and schemas

QUERIES = {
    "A1": "SELECT customer_id, COUNT(*) AS n, SUM(amount_cents) AS total FROM orders GROUP BY customer_id ORDER BY customer_id",
    "A2": "SELECT COUNT(DISTINCT customer_id) AS n FROM orders",
    "S1": "SELECT COUNT(*) AS n, COALESCE(SUM(amount_cents), 0) AS total FROM orders WHERE message LIKE 'needle%'",
    "S2": "SELECT COUNT(*) AS n, COALESCE(SUM(amount_cents), 0) AS total FROM orders WHERE message LIKE '%search%'",
    "T1": ("SELECT status, COUNT(*) AS n, SUM(amount_cents) AS total FROM orders WHERE created_at >= '%s' "
           "AND created_at < '%s' GROUP BY status ORDER BY status") % (LOWER, UPPER),
    "J1": ("SELECT c.country, COUNT(*) AS n, SUM(o.amount_cents) AS total FROM orders o "
           "JOIN customers c ON o.customer_id=c.id GROUP BY c.country ORDER BY c.country"),
    "C1": "SELECT SUM(amount_cents) AS total FROM orders",
}

SQLITE_SCHEMA = ("CREATE TABLE customers(id INTEGER, country TEXT); CREATE TABLE orders(id INTEGER, "
                 "customer_id INTEGER, amount_cents INTEGER, status TEXT, created_at TEXT, message TEXT);")

SCHEMAS = {
    "clickhouse": [
        "CREATE TABLE customers (id Int64, country String) ENGINE=MergeTree ORDER BY id",
        "CREATE TABLE orders (id Int64, customer_id Int64, amount_cents Int64, status String, "
        "created_at DateTime('UTC'), message String) ENGINE=MergeTree ORDER BY (created_at,id)"],
    "postgres": [
        'CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, country TEXT COLLATE "C" NOT NULL)',
        'CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, customer_id BIGINT NOT NULL, '
        'amount_cents BIGINT NOT NULL, status TEXT COLLATE "C" NOT NULL, '
        'created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, message TEXT COLLATE "C" NOT NULL)'],
    "mariadb": [
        "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, country VARCHAR(2) NOT NULL) "
        "ENGINE=InnoDB DEFAULT CHARSET=ascii COLLATE=ascii_bin",
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, customer_id BIGINT NOT NULL, "
        "amount_cents BIGINT NOT NULL, status VARCHAR(16) NOT NULL, created_at DATETIME NOT NULL, "
        "message VARCHAR(128) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=ascii COLLATE=ascii_bin"],
}
# Row stores build their secondary indexes after the bulk load; the cost is timed separately.
INDEXES = {
    "clickhouse": [],
    "postgres": ["CREATE INDEX orders_customer_idx ON orders(customer_id)",
                 "CREATE INDEX orders_created_idx ON orders(created_at)"],
    "mariadb": ["CREATE INDEX orders_customer_idx ON orders(customer_id)",
                "CREATE INDEX orders_created_idx ON orders(created_at)"],
}

# ---------------------------------------------------------------- schedule

ENGINES = ("clickhouse_old", "clickhouse_new", "postgres", "mariadb")


def schedule(rounds=8, seed=1177):
    if rounds < 4 or rounds % 4:
        raise ValueError("Rounds must be a positive multiple of four")
    rng = random.Random(seed)
    labels = list(ENGINES)
    rng.shuffle(labels)
    out = []
    # Williams design: each engine takes each position, and each directed adjacent
    # pair occurs equally often in every block of four rounds.
    for _ in range(rounds // 4):
        for shift in range(4):
            out.append({"round": len(out) + 1, "engines": [labels[(i + shift) % 4] for i in (0, 1, 3, 2)]})
    return out


# ---------------------------------------------------------------- measurement plan

# Scenarios change one factor at a time. Customer counts are clipped to the row count.
def scenario_plan(rows):
    def sc(customers, ppm=10000, skew=False):
        return Scenario(rows=rows, customers=min(customers, rows), selectivity_ppm=ppm, skew=skew)
    plan = {
        "c1k": (sc(1000), TESTS),
        "c100k": (sc(100000), ("A1", "A2", "J1")),
        "c1m": (sc(1000000), ("A1", "A2", "J1")),
        "sel01": (sc(1000, 1000), ("S1", "S2")),
        "sel10": (sc(1000, 100000), ("S1", "S2")),
        "skew": (sc(100000, skew=True), ("J1",)),
    }
    seen = {}
    for name, (s, tests) in list(plan.items()):
        key = (s, tests)
        if key in seen:      # at small scale c1m collapses onto c100k
            del plan[name]
        seen[key] = name
    return plan


def normalise(rows):
    """Canonical JSON-ready rows: integers stay exact, decimals from SUM become int."""
    out = []
    for row in rows:
        r = []
        for v in row:
            if isinstance(v, str):
                r.append(int(v) if re.fullmatch(r"-?\d+", v) else v)
            elif isinstance(v, (int,)) or v is None:
                r.append(v)
            else:
                iv = int(v)
                if iv != v:
                    raise ValueError("Non-integral result value")
                r.append(iv)
        out.append(r)
    return out


def t2_windows(rows_scenario):
    """Fixed, clock independent moving window grid for the ClickHouse condition cache diagnostic."""
    base = datetime(2026, 9, 25)
    return [((base + timedelta(hours=6 * k)).strftime("%Y-%m-%d %H:%M:%S"),
             (base + timedelta(hours=6 * k, days=7)).strftime("%Y-%m-%d %H:%M:%S")) for k in range(12)]


T2_SQL = ("SELECT COUNT(*), COALESCE(SUM(amount_cents), 0) FROM orders WHERE created_at >= '%s' "
          "AND created_at < '%s' AND message LIKE '%%search%%'")


def t2_oracle(orders_csv, windows):
    out = [[0, 0] for _ in windows]
    for _, _, cents, _, ts, message in read_orders(orders_csv):
        if "search" not in message:
            continue
        for i, (lo, hi) in enumerate(windows):
            if lo <= ts < hi:
                out[i][0] += 1
                out[i][1] += cents
    return out


# ---------------------------------------------------------------- selftest

def verify_sql(data):
    manifest, expected = verify(data)
    # SQLite is only a portable SQL semantics check, never a benchmark baseline.
    con = sqlite3.connect(":memory:")
    try:
        con.executescript(SQLITE_SCHEMA)
        with (Path(data) / "customers.csv").open(newline="") as f:
            con.executemany("INSERT INTO customers VALUES (?,?)", ((int(i), c) for i, c in csv.reader(f)))
        con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?)", read_orders(Path(data) / "orders.csv"))
        for test in TESTS:
            actual = [list(row) for row in con.execute(QUERIES[test])]
            if canonical_hash(actual) != canonical_hash(expected[test]):
                raise ValueError("SQL/oracle mismatch: " + test)
        return {"status": "passed", "queries": list(TESTS), "rows": manifest["scenario"]["rows"]}
    finally:
        con.close()


class SuiteTests(unittest.TestCase):
    def test_deterministic_bytes(self):
        with tempfile.TemporaryDirectory() as d:
            a = generate(Path(d) / "a", Scenario(rows=103, customers=17))
            b = generate(Path(d) / "b", Scenario(rows=103, customers=17))
            self.assertEqual(a, b)

    def test_sql_oracle_variants(self):
        with tempfile.TemporaryDirectory() as d:
            cases = [(103, 17, 0, False), (1000, 1000, 1000, False), (1001, 13, 100000, True), (1, 1, 500000, False)]
            for i, (rows, customers, ppm, skew) in enumerate(cases):
                dest = Path(d) / str(i)
                m = generate(dest, Scenario(rows, customers, ppm, skew=skew))
                self.assertEqual(m["prefix_matches"], rows * ppm // 1000000)
                self.assertEqual(m["substring_matches"], rows * ppm // 1000000)
                self.assertEqual(m["distinct_customers"], customers)
                self.assertEqual(verify_sql(dest)["status"], "passed")

    def test_window_boundaries(self):
        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / "data"
            generate(dest, Scenario(rows=4, customers=2))
            _, answers = verify(dest)
            self.assertEqual(sum(r[1] for r in answers["T1"]), 2)

    def test_exact_large_integer_hash(self):
        self.assertNotEqual(canonical_hash([[2 ** 53]]), canonical_hash([[2 ** 53 + 1]]))
        self.assertNotEqual(canonical_hash([[1, 2]]), canonical_hash([[1], [2]]))

    def test_detect_file_tampering(self):
        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / "data"
            generate(dest, Scenario(rows=10, customers=2))
            with (dest / "orders.csv").open("a") as f:
                f.write("corrupt\n")
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                verify(dest)

    def test_no_overwrite(self):
        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / "data"
            generate(dest, Scenario(rows=10, customers=2))
            with self.assertRaises(FileExistsError):
                generate(dest, Scenario(rows=10, customers=2))

    def test_bad_scenarios(self):
        for s in [Scenario(rows=0), Scenario(rows=5, customers=6),
                  Scenario(rows=5, customers=1, selectivity_ppm=-1),
                  Scenario(rows=5, customers=1, selectivity_ppm=500001)]:
            with self.assertRaises(ValueError):
                s.validate()

    def test_selectivity_does_not_change_other_fields(self):
        a = list(order_rows(Scenario(rows=100, customers=7, selectivity_ppm=0)))
        b = list(order_rows(Scenario(rows=100, customers=7, selectivity_ppm=100000)))
        self.assertEqual([r[:-1] for r in a], [r[:-1] for r in b])

    def test_skew_keeps_time_money_text_fixed(self):
        a = list(order_rows(Scenario(rows=100, customers=7)))
        b = list(order_rows(Scenario(rows=100, customers=7, skew=True)))
        self.assertEqual([r[2:] for r in a], [r[2:] for r in b])
        self.assertGreater(sum(r[1] == 1 for r in b), sum(r[1] == 1 for r in a))

    def test_join_uses_actual_dimension(self):
        self.assertEqual(oracle([(1, 1, 7, "paid", "2026-10-20 00:00:00", "x" * 128)], {1: "zz"})["J1"], [["zz", 1, 7]])
        with self.assertRaisesRegex(ValueError, "Unresolved"):
            oracle([(1, 2, 7, "paid", "2026-10-20 00:00:00", "x" * 128)], {1: "zz"})

    def test_rehashed_customer_change_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / "data"
            generate(dest, Scenario(rows=10, customers=2))
            f = dest / "customers.csv"
            f.write_text(f.read_text().replace("au", "zz"))
            m = json.loads((dest / "manifest.json").read_text())
            m["files"]["customers.csv"] = sha256(f)
            (dest / "manifest.json").write_text(json.dumps(m))
            with self.assertRaisesRegex(ValueError, "Expected answers"):
                verify(dest)

    def test_duplicate_dimension_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / "customers.csv"
            f.write_text("1,au\n1,us\n")
            with self.assertRaisesRegex(ValueError, "duplicate"):
                read_customers(f)

    def test_metadata_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / "data"
            generate(dest, Scenario(rows=10, customers=2))
            m = json.loads((dest / "manifest.json").read_text())
            m["scenario"]["rows"] = 11
            (dest / "manifest.json").write_text(json.dumps(m))
            with self.assertRaisesRegex(ValueError, "metadata"):
                verify(dest)

    def test_all_text_profiles(self):
        with tempfile.TemporaryDirectory() as d:
            for profile in ("mixed", "random", "repetitive"):
                dest = Path(d) / profile
                m = generate(dest, Scenario(rows=1000, customers=17, selectivity_ppm=100000, text_profile=profile))
                self.assertEqual(m["prefix_matches"], 100)
                self.assertEqual(m["substring_matches"], 100)
                self.assertEqual(verify_sql(dest)["status"], "passed")

    def test_schedule_balance(self):
        from collections import Counter
        runs = schedule()
        self.assertEqual(runs, schedule())
        for position in range(4):
            self.assertEqual(Counter(r["engines"][position] for r in runs), Counter({e: 2 for e in ENGINES}))
        pairs = Counter((a, b) for r in runs for a, b in zip(r["engines"], r["engines"][1:]))
        self.assertEqual(set(pairs.values()), {2})
        with self.assertRaises(ValueError):
            schedule(5)

    def test_normalise_exact(self):
        from decimal import Decimal
        self.assertEqual(normalise([("5", Decimal(2 ** 60 + 1), "us")]), [[5, 2 ** 60 + 1, "us"]])
        with self.assertRaises(ValueError):
            normalise([(Decimal("1.5"),)])

    def test_scenario_plan_small_scale(self):
        plan = scenario_plan(100000)
        self.assertNotIn("c1m", plan)       # clipped onto c100k, never run twice
        self.assertEqual(set(plan["c1k"][1]), set(TESTS))
        for s, _ in plan.values():
            s.validate()

    def test_t2_oracle_matches_sql(self):
        with tempfile.TemporaryDirectory() as d:
            dest = Path(d) / "t"
            generate(dest, Scenario(rows=3000, customers=30, selectivity_ppm=100000))
            con = sqlite3.connect(":memory:")
            con.executescript(SQLITE_SCHEMA)
            con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?)", read_orders(dest / "orders.csv"))
            wins = t2_windows(None)
            want = t2_oracle(dest / "orders.csv", wins)
            got = [list(con.execute(T2_SQL % w).fetchone()) for w in wins]
            self.assertEqual(got, want)
            self.assertGreater(sum(w[0] for w in want), 0)


class ReportTests(unittest.TestCase):
    def rows(self):
        out = []
        for rnd in "12":
            for eng, ms in (("clickhouse_old", 10.0 * int(rnd)), ("clickhouse_new", 5.0 * int(rnd))):
                for i in range(3):
                    out.append(dict(round=rnd, position="1", engine=eng, scenario="c1k", test="S1", phase="warm",
                                    run=str(i), ms=str(ms), status="ok", hash_ok="1", cpu_us="", io_read_bytes="",
                                    peak_anon_bytes="", ch_query_memory="", ch_read_rows=""))
        return out

    def test_paired_ratio_cancels_round_drift(self):
        # Round 2 is twice as slow for both builds; the paired ratio stays exactly 0.5.
        (med, lo, hi, n), = paired_ratios(self.rows()).values()
        self.assertEqual((med, lo, hi, n), (0.5, 0.5, 0.5, 2))

    def test_failed_runs_excluded_from_pairing(self):
        rows = self.rows()
        for r in rows:
            if r["round"] == "2":
                r["status"] = "timeout"
        (_, _, _, n), = paired_ratios(rows).values()
        self.assertEqual(n, 1)

    def test_report_roundtrip_and_merge(self):
        with tempfile.TemporaryDirectory() as d:
            csv_path, txt = Path(d) / "results.csv", Path(d) / "results.txt"
            with csv_path.open("w", newline="") as f:
                w = csv.DictWriter(f, FIELDS)
                w.writeheader()
                w.writerows(self.rows())
            merge_ch_memory(csv_path, {("1", "1", "clickhouse_old", "c1k", "S1", "warm", "0"): ("2097152", "1000")})
            self.assertEqual(sum(r["ch_query_memory"] == "2097152" for r in load_results(csv_path)), 1)
            write_report(csv_path, [], {"rows": 1}, txt)
            ns = argparse.Namespace(csv=str(csv_path), txt=str(txt))
            cmd_report(ns)
            self.assertIn("UPGRADE RATIO", txt.read_text())
            self.assertIn("0.500 [0.500-0.500] n=2", txt.read_text())


def cmd_selftest(_args):
    suite = unittest.TestSuite([unittest.defaultTestLoader.loadTestsFromTestCase(c) for c in (SuiteTests, ReportTests)])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


# ---------------------------------------------------------------- environment and containers

IMAGES = {
    "clickhouse_old": "docker.io/clickhouse/clickhouse-server:26.9.27.3",
    "clickhouse_new": "docker.io/clickhouse/clickhouse-server:26.10.3.7",
    "postgres": "docker.io/library/postgres:18.6",
    "mariadb": "docker.io/library/mariadb:11.8.9",
}
FAMILY = {"clickhouse_old": "clickhouse", "clickhouse_new": "clickhouse",
          "postgres": "postgres", "mariadb": "mariadb"}
PORTS = {"clickhouse_old": 18123, "clickhouse_new": 18123, "postgres": 15432, "mariadb": 13306}
PASSWORD = "benchpass"
TIMEOUT_S = 120
SERVER_ARGS = {
    "postgres": ["postgres", "-c", "shared_buffers=2GB", "-c", "effective_cache_size=6GB",
                 "-c", "work_mem=256MB", "-c", "max_parallel_workers_per_gather=4",
                 "-c", "max_parallel_workers=8", "-c", "max_worker_processes=8", "-c", "jit=off",
                 "-c", "max_wal_size=8GB", "-c", "checkpoint_timeout=30min", "-c", "max_connections=20"],
    "mariadb": ["--innodb-buffer-pool-size=4G", "--innodb-log-file-size=2G", "--secure-file-priv=/data",
                "--max-connections=20", "--skip-name-resolve"],
}
DATA_MOUNT = {"clickhouse": "/var/lib/clickhouse", "postgres": "/var/lib/postgresql",
              "mariadb": "/var/lib/mysql"}
CH_SETTINGS = {"use_query_cache": "0", "use_query_condition_cache": "1", "max_execution_time": str(TIMEOUT_S)}


def sh(cmd, check=True, timeout=None, text=True, stdin=None):
    r = subprocess.run(cmd, capture_output=True, text=text, timeout=timeout, input=stdin)
    if check and r.returncode:
        raise RuntimeError("command failed (%s): %s\n%s" % (r.returncode, " ".join(map(str, cmd))[:300], (r.stderr or "")[-800:]))
    return r


class Podman:
    def __init__(self, prefix):
        self.prefix = prefix

    def __call__(self, *args, **kw):
        return sh(self.prefix + list(args), **kw)


def topology():
    """Return list of physical cores, each a sorted list of SMT sibling CPU ids."""
    seen, cores = set(), []
    for cpu in sorted(int(p.name[3:]) for p in Path("/sys/devices/system/cpu").glob("cpu[0-9]*")):
        f = Path("/sys/devices/system/cpu/cpu%d/topology/thread_siblings_list" % cpu)
        sib = f.read_text().strip() if f.exists() else str(cpu)
        if sib in seen:
            continue
        seen.add(sib)
        ids = []
        for part in sib.split(","):
            a, _, b = part.partition("-")
            ids += list(range(int(a), int(b or a) + 1))
        cores.append(sorted(ids))
    return cores


def cpuset(cores):
    return ",".join(str(c) for core in cores for c in core)


def environment(podman, args, cores):
    def read(path):
        try:
            return Path(path).read_text().strip()
        except OSError:
            return "unknown"
    mem = dict(l.split(":", 1) for l in Path("/proc/meminfo").read_text().splitlines())
    cpu = next((l.split(":", 1)[1].strip() for l in Path("/proc/cpuinfo").read_text().splitlines()
                if l.startswith("model name")), "unknown")
    govs = sorted({read(p) for p in Path("/sys/devices/system/cpu").glob("cpu[0-9]*/cpufreq/scaling_governor")})
    return {"started_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "host": platform.node(), "kernel": platform.release(), "cpu": cpu,
            "physical_cores": len(cores), "smt_threads": sum(len(c) for c in cores),
            "governor": ",".join(govs), "mem_total": mem["MemTotal"].strip(),
            "mem_available": mem["MemAvailable"].strip(), "swap_total": mem["SwapTotal"].strip(),
            "swap_free": mem["SwapFree"].strip(), "podman": podman("--version").stdout.strip(),
            "python": platform.python_version(), "bench_sha256": sha256(Path(__file__)),
            "server_cpus": args.server_cpus, "client_cpus": args.client_cpus,
            "server_mem": args.server_mem, "timeout_s": TIMEOUT_S, "rows": args.rows,
            "rounds": args.rounds, "warmups": args.warmups, "measured": args.measured}


class Server:
    """One engine container with a persistent volume, hard CPU and memory limits."""

    def __init__(self, podman, name, args, volume, datadir):
        self.p, self.name, self.args, self.volume, self.datadir = podman, name, args, volume, datadir
        self.family = FAMILY[name]
        self.cid = None
        self.cgroup = None

    def start(self):
        env = {"clickhouse": ["-e", "CLICKHOUSE_USER=bench", "-e", "CLICKHOUSE_PASSWORD=" + PASSWORD,
                              "-e", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1"],
               "postgres": ["-e", "POSTGRES_PASSWORD=" + PASSWORD],
               "mariadb": ["-e", "MARIADB_ROOT_PASSWORD=" + PASSWORD]}[self.family]
        cmd = ["run", "-d", "--name", "chr_" + self.name, "--cpuset-cpus", self.args.server_cpus,
               "--memory", self.args.server_mem, "--memory-swap", self.args.server_mem, "--shm-size", "1g",
               "-p", "127.0.0.1:%d:%d" % (PORTS[self.name], {"clickhouse": 8123, "postgres": 5432,
                                                           "mariadb": 3306}[self.family]),
               "-v", "%s:%s" % (self.volume, DATA_MOUNT[self.family]),
               "-v", "%s:/data:ro,z" % self.datadir, *env, IMAGES[self.name]]
        if self.family in SERVER_ARGS:
            cmd += SERVER_ARGS[self.family]
        self.cid = self.p(*cmd).stdout.strip()
        pid = self.p("inspect", "-f", "{{.State.Pid}}", self.cid).stdout.strip()
        rel = Path("/proc/%s/cgroup" % pid).read_text().strip().split("::", 1)[1]
        self.cgroup = Path("/sys/fs/cgroup" + rel)

    def stop(self):
        if self.cid:
            self.p("rm", "-f", "-t", "30", self.cid, check=False)
            self.cid = None

    def exec(self, *cmd, stdin=None, timeout=None):
        return self.p("exec", "-i", self.cid, *cmd, stdin=stdin, timeout=timeout)

    def counters(self):
        """Cumulative cgroup CPU microseconds, physical read bytes and current anonymous plus shared memory."""
        cpu = io = anon = 0
        try:
            for l in (self.cgroup / "cpu.stat").read_text().splitlines():
                if l.startswith("usage_usec"):
                    cpu = int(l.split()[1])
            for l in (self.cgroup / "io.stat").read_text().splitlines():
                for tok in l.split()[1:]:
                    if tok.startswith("rbytes="):
                        io += int(tok[7:])
            for l in (self.cgroup / "memory.stat").read_text().splitlines():
                # Shared memory counts: PostgreSQL keeps its buffers there, not in anon.
                if l.startswith(("anon ", "shmem ")):
                    anon += int(l.split()[1])
        except OSError:
            pass
        return cpu, io, anon


class Sampler:
    """Polls anonymous plus shared container memory; the peak is a sampled lower bound, page cache excluded."""

    def __init__(self, server, interval=0.02):
        self.server, self.interval, self.peak = server, interval, 0
        self.stop_flag = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        while not self.stop_flag.is_set():
            self.peak = max(self.peak, self.server.counters()[2])
            time.sleep(self.interval)

    def __enter__(self):
        self.peak = self.server.counters()[2]
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop_flag.set()
        self.thread.join()


# ---------------------------------------------------------------- clients

class ClickHouseClient:
    def __init__(self, port, database, settings=None):
        self.port, self.database = port, database
        self.settings = dict(CH_SETTINGS, **(settings or {}))
        self.conn = http.client.HTTPConnection("127.0.0.1", port, timeout=TIMEOUT_S + 10)
        self.last_id = None

    def query(self, sql, query_id=None, fmt="TabSeparated", settings=None, database=True):
        params = dict(self.settings, **(settings or {}))
        if database:
            params["database"] = self.database
        if query_id:
            params["query_id"] = query_id
        self.conn.request("POST", "/?" + urllib.parse.urlencode(params), body=sql.encode() + (b" FORMAT " + fmt.encode() if fmt else b""),
                          headers={"X-ClickHouse-User": "bench", "X-ClickHouse-Key": PASSWORD})
        resp = self.conn.getresponse()
        body = resp.read()
        if resp.status != 200:
            raise RuntimeError(body.decode(errors="replace")[:400])
        return body

    def rows(self, sql, **kw):
        text = self.query(sql, **kw).decode()
        return [line.split("\t") for line in text.splitlines()]

    def run(self, sql, query_id=None):
        return self.rows(sql, query_id=query_id)

    def close(self):
        self.conn.close()


class PostgresClient:
    def __init__(self, port, database):
        import psycopg
        self.con = psycopg.connect(host="127.0.0.1", port=port, user="postgres", password=PASSWORD,
                                   dbname="postgres", autocommit=True,
                                   options="-c search_path=%s -c statement_timeout=%d" % (database, TIMEOUT_S * 1000))

    def run(self, sql, query_id=None):
        with self.con.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall() if cur.description else []

    def close(self):
        self.con.close()


class MariaClient:
    def __init__(self, port, database):
        import pymysql
        self.con = pymysql.connect(host="127.0.0.1", port=port, user="root", password=PASSWORD,
                                   database=database, autocommit=True, read_timeout=TIMEOUT_S + 10)
        with self.con.cursor() as cur:
            cur.execute("SET SESSION max_statement_time=%d" % TIMEOUT_S)

    def run(self, sql, query_id=None):
        with self.con.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall() if cur.description else []

    def close(self):
        self.con.close()


def admin_client(server, database=None):
    port = PORTS[server.name]
    if server.family == "clickhouse":
        return ClickHouseClient(port, database or "default")
    if server.family == "postgres":
        return PostgresClient(port, database or "public")
    return MariaClient(port, database)


def wait_ready(server, seconds=180):
    end = time.time() + seconds
    last = None
    while time.time() < end:
        try:
            c = admin_client(server)
            c.run("SELECT 1")
            c.close()
            return
        except Exception as e:      # connection refused until the server finishes starting
            last = e
            time.sleep(1)
    raise RuntimeError("server %s not ready: %s" % (server.name, last))


# ---------------------------------------------------------------- loading (L1)

def load_scenario(server, label, scenario_name, data_rel, rows):
    """Create schema, bulk load, build indexes, settle. Returns timing and size dict."""
    fam = server.family
    db = "s_" + scenario_name
    out = {"scenario": scenario_name, "engine": server.name}
    a = admin_client(server)
    if fam == "clickhouse":
        a.run("CREATE DATABASE " + db)
        c = ClickHouseClient(PORTS[server.name], db)
        ddl = lambda s: c.query(s)
    elif fam == "postgres":
        a.run("CREATE SCHEMA " + db)
        c = PostgresClient(PORTS[server.name], db)
        ddl = c.run
    else:
        a.run("CREATE DATABASE " + db)
        c = MariaClient(PORTS[server.name], db)
        ddl = c.run
    a.close()
    for stmt in SCHEMAS[fam]:
        ddl(stmt)
    t0 = time.perf_counter()
    for table in ("customers", "orders"):
        path = "/data/%s/%s.csv" % (data_rel, table)
        if fam == "clickhouse":
            server.exec("sh", "-c", "clickhouse-client --user bench --password %s --database %s "
                        "--query 'INSERT INTO %s FORMAT CSV' < %s" % (PASSWORD, db, table, path))
        elif fam == "postgres":
            c.run("COPY %s FROM '%s' (FORMAT csv)" % (table, path))
        else:
            c.run("LOAD DATA INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "
                  "LINES TERMINATED BY '\\n'" % (path, table))
    out["load_s"] = round(time.perf_counter() - t0, 3)
    t0 = time.perf_counter()
    for stmt in INDEXES[fam]:
        ddl(stmt)
    out["index_s"] = round(time.perf_counter() - t0, 3)
    t0 = time.perf_counter()
    out.update(settle(server, c, db))
    out["settle_s"] = round(time.perf_counter() - t0, 3)
    if not out["settled"]:
        raise RuntimeError("%s did not settle after load of %s" % (server.name, scenario_name))
    n = int(normalise(c.run("SELECT COUNT(*) FROM orders") if fam != "clickhouse" else c.rows("SELECT COUNT(*) FROM orders"))[0][0])
    if n != rows:
        raise RuntimeError("row count mismatch after load: %d != %d" % (n, rows))
    c.close()
    return out


def settle(server, c, db):
    """Reach a declared stable state and report stored bytes. Never runs OPTIMIZE FINAL."""
    fam = server.family
    if fam == "clickhouse":
        stable, last = 0, None
        for _ in range(300):
            active = int(c.rows("SELECT count() FROM system.merges")[0][0])
            parts = int(c.rows("SELECT count() FROM system.parts WHERE active AND database='%s'" % db)[0][0])
            stable = stable + 1 if (active == 0 and parts == last) else 0
            last = parts
            if stable >= 3:
                break
            time.sleep(2)
        size = int(c.rows("SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database='%s'" % db)[0][0])
        return {"stored_bytes": size, "parts": last, "settled": stable >= 3}
    if fam == "postgres":
        # VACUUM cannot run in a transaction block; the client connection is autocommit.
        with c.con.cursor() as cur:
            for t in ("customers", "orders"):
                cur.execute("VACUUM (ANALYZE) " + t)
            cur.execute("CHECKPOINT")
            cur.execute("SELECT pg_total_relation_size('orders') + pg_total_relation_size('customers')")
            return {"stored_bytes": int(cur.fetchone()[0]), "settled": True}
    with c.con.cursor() as cur:
        for t in ("customers", "orders"):
            cur.execute("ANALYZE TABLE " + t)
            cur.fetchall()
        # InnoDB flushes lazily, so dirty pages would never drain on their own. Force it
        # for the settle window, then restore the server defaults.
        cur.execute("SET GLOBAL innodb_max_dirty_pages_pct_lwm=0")
        cur.execute("SET GLOBAL innodb_max_dirty_pages_pct=0")
        settled = False
        for _ in range(150):
            cur.execute("SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_pages_dirty'")
            if int(cur.fetchone()[1]) < 100:
                settled = True
                break
            time.sleep(2)
        cur.execute("SET GLOBAL innodb_max_dirty_pages_pct=90")
        cur.execute("SELECT SUM(data_length+index_length) FROM information_schema.tables WHERE table_schema=%s", (db,))
        return {"stored_bytes": int(cur.fetchone()[0]), "settled": settled}


# ---------------------------------------------------------------- timed runs

FIELDS = ["round", "position", "engine", "scenario", "test", "phase", "run", "ms", "status", "hash_ok",
          "cpu_us", "io_read_bytes", "peak_anon_bytes", "ch_query_memory", "ch_read_rows"]


def timed(server, client, sql, expected_hash, query_id):
    """One timed execution through full result consumption, with correctness checked afterwards."""
    cpu0, io0, _ = server.counters()
    status, ok = "ok", False
    with Sampler(server) as sm:
        t0 = time.perf_counter()
        try:
            rows = client.run(sql, query_id=query_id)
            ms = (time.perf_counter() - t0) * 1000
        except Exception as e:
            ms = (time.perf_counter() - t0) * 1000
            rows = None
            msg = str(e).lower()
            status = "timeout" if ("timeout" in msg or "timed out" in msg or "max_statement_time" in msg
                                   or "statement timeout" in msg or "time exceeded" in msg or ms >= TIMEOUT_S * 1000) \
                else "error:" + str(e).replace("\n", " ")[:120]
    cpu1, io1, _ = server.counters()
    if rows is not None:
        ok = canonical_hash(normalise(rows)) == expected_hash
        if not ok:
            status = "mismatch"
    return {"ms": round(ms, 3), "status": status, "hash_ok": int(ok), "cpu_us": cpu1 - cpu0,
            "io_read_bytes": io1 - io0, "peak_anon_bytes": sm.peak}


def run_cell(server, client, scenario, test, expected_hash, rnd, pos, args, writer, qid_base):
    """First run, warmups, then measured runs. A failed or timed out cell stops early."""
    sql = QUERIES[test]
    plan = [("first", 0)] + [("warmup", i + 1) for i in range(args.warmups)] + \
           [("warm", i + 1) for i in range(args.measured)]
    ids = []
    for phase, i in plan:
        qid = "%s-%s-%s-%d" % (qid_base, scenario, test, len(ids))
        r = timed(server, client, sql, expected_hash, qid)
        ids.append((qid, phase, i, r))
        writer(dict(round=rnd, position=pos, engine=server.name, scenario=scenario, test=test,
                    phase=phase, run=i, ch_query_memory="", ch_read_rows="", **r))
        if r["status"] != "ok":
            break
    return ids


def ch_log_backfill(client, rows_by_id):
    """Fill ClickHouse query memory and rows read from the query log after a block."""
    client.query("SYSTEM FLUSH LOGS", fmt=None, database=False)
    for qid, d in rows_by_id.items():
        res = client.rows("SELECT memory_usage, read_rows FROM system.query_log WHERE query_id='%s' "
                          "AND type='QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1" % qid, database=False)
        if res:
            d["ch_query_memory"], d["ch_read_rows"] = res[0][0], res[0][1]


def run_t2(server, client, scenario, orders_csv_expected, rnd, pos, args, writer, qid_base):
    """Condition cache diagnostic: same moving window grid, cache off then on, fresh cache each arm."""
    wins = t2_windows(None)
    for arm, setting in (("cache_off", "0"), ("cache_on", "1")):
        client.query("SYSTEM DROP QUERY CONDITION CACHE", fmt=None, database=False)
        for k, (lo, hi) in enumerate(wins):
            sql = T2_SQL % (lo, hi)
            qid = "%s-%s-T2%s-%d" % (qid_base, scenario, arm, k)
            client.settings["use_query_condition_cache"] = setting
            t0 = time.perf_counter()
            try:
                rows = client.rows(sql, query_id=qid)
                ms = (time.perf_counter() - t0) * 1000
                ok = normalise(rows) == [orders_csv_expected[k]]
                status = "ok" if ok else "mismatch"
            except Exception as e:
                ms, ok, status = (time.perf_counter() - t0) * 1000, False, "error:" + str(e).replace("\n", " ")[:120]
            writer(dict(round=rnd, position=pos, engine=server.name, scenario=scenario, test="T2",
                        phase=arm, run=k, ms=round(ms, 3), status=status, hash_ok=int(ok), cpu_us="",
                        io_read_bytes="", peak_anon_bytes="", ch_query_memory="", ch_read_rows=""))
    client.settings["use_query_condition_cache"] = CH_SETTINGS["use_query_condition_cache"]


def verify_ch_settings(client):
    """Confirm the applied cache and distinct settings instead of assuming them."""
    got = {r[0]: r[1] for r in client.rows(
        "SELECT name, value FROM system.settings WHERE name IN ('use_query_cache','use_query_condition_cache',"
        "'count_distinct_implementation')", database=False)}
    if got.get("count_distinct_implementation") not in ("uniqExact",):
        raise RuntimeError("COUNT(DISTINCT) is not exact: %r" % got)
    return got


# ---------------------------------------------------------------- reporting

def load_results(path):
    with Path(path).open(newline="") as f:
        return list(csv.DictReader(f))


def quartiles(v):
    v = sorted(v)
    if len(v) < 2:
        return (v[0], v[0]) if v else (None, None)
    q = statistics.quantiles(v, n=4, method="inclusive")
    return q[0], q[2]


def summarise(rows):
    """Median of per round medians per (scenario, test, engine), over valid warm runs only."""
    per = defaultdict(lambda: defaultdict(list))
    bad = defaultdict(int)
    for r in rows:
        if r["test"] == "T2":
            continue
        key = (r["scenario"], r["test"], r["engine"])
        if r["phase"] == "warm":
            if r["status"] == "ok" and r["hash_ok"] == "1":
                per[key][r["round"]].append(float(r["ms"]))
            else:
                bad[key] += 1
    out = {}
    for key, rounds in per.items():
        meds = [statistics.median(v) for v in rounds.values()]
        q1, q3 = quartiles(meds)
        out[key] = {"median": statistics.median(meds), "min": min(meds), "max": max(meds), "q1": q1, "q3": q3,
                    "rounds": len(meds), "bad": bad[key]}
    for key, n in bad.items():
        out.setdefault(key, {"median": None, "rounds": 0, "bad": n})
    return out


def paired_ratios(rows):
    """Per round new/old ratio of warm medians, so round-level drift cancels."""
    per = defaultdict(lambda: defaultdict(list))
    for r in rows:
        if r["test"] != "T2" and r["phase"] == "warm" and r["status"] == "ok" and r["hash_ok"] == "1" \
                and r["engine"] in ("clickhouse_old", "clickhouse_new"):
            per[(r["scenario"], r["test"], r["engine"])][r["round"]].append(float(r["ms"]))
    out = {}
    for sc, t in sorted({(k[0], k[1]) for k in per}):
        old, new = per.get((sc, t, "clickhouse_old"), {}), per.get((sc, t, "clickhouse_new"), {})
        ratios = [statistics.median(new[k]) / statistics.median(old[k]) for k in sorted(set(old) & set(new))
                  if statistics.median(old[k]) > 0]
        if ratios:
            out[(sc, t)] = (statistics.median(ratios), min(ratios), max(ratios), len(ratios))
    return out


def fmt(x):
    return "-" if x is None else ("%.1f" % x if x < 1000 else "%.0f" % x)


META_MARK = "RAW METADATA (JSON, used by the report command)"


def write_report(results_csv, loads, env, txt_path):
    rows = load_results(results_csv)
    s = summarise(rows)
    lines = ["ClickHouse 26.10 review: results", "", "ENVIRONMENT"]
    lines += ["  %-14s %s" % (k, v) for k, v in env.items()]
    lines += ["", "IMAGES (pinned tags and digests)"]
    lines += ["  %-15s %s" % (k, v) for k, v in env.get("images", {}).items()] if isinstance(env.get("images"), dict) else []
    lines += ["", "WARM LATENCY, ms. Median of per-round medians; [min-max] over rounds; rounds used."]
    lines += ["  Failed, timed out or mismatching executions are excluded here and counted in 'bad'.", ""]
    cells = sorted({(k[0], k[1]) for k in s})
    engines = list(ENGINES)
    lines.append("  %-8s %-4s " % ("scenario", "test") + " ".join("%-26s" % e for e in engines))
    for sc, t in cells:
        cols = []
        for e in engines:
            v = s.get((sc, t, e))
            if not v or v["median"] is None:
                cols.append("%-26s" % ("failed (bad=%d)" % v["bad"] if v else "-"))
            else:
                cols.append("%-26s" % ("%s [%s-%s] n=%d%s" % (fmt(v["median"]), fmt(v["min"]), fmt(v["max"]), v["rounds"],
                                                           " bad=%d" % v["bad"] if v["bad"] else "")))
        lines.append("  %-8s %-4s " % (sc, t) + " ".join(cols))
    lines += ["", "UPGRADE RATIO (clickhouse_new / clickhouse_old, below 1 is faster)",
              "  Median over rounds of the per-round ratio; [min-max] over rounds; rounds paired."]
    for (sc, t), (med, lo, hi, n) in paired_ratios(rows).items():
        lines.append("  %-8s %-4s %.3f [%.3f-%.3f] n=%d" % (sc, t, med, lo, hi, n))
    qm = defaultdict(list)
    for r in rows:
        if r["test"] != "T2" and r["phase"] == "warm" and r["status"] == "ok" and r["ch_query_memory"] != "":
            qm[(r["scenario"], r["test"], r["engine"])].append((int(r["ch_query_memory"]), int(r["ch_read_rows"] or 0)))
    if qm:
        lines += ["", "CLICKHOUSE OWN TRACKER (not comparable to other engines): median query memory MiB / rows read"]
        for (sc, t, e), v in sorted(qm.items()):
            lines.append("  %-8s %-4s %-15s %8.1f MiB  %d rows" % (
                sc, t, e, statistics.median(m for m, _ in v) / 2 ** 20, statistics.median(r_ for _, r_ in v)))
    t2 = defaultdict(list)
    for r in rows:
        if r["test"] == "T2" and r["status"] == "ok":
            t2[(r["engine"], r["phase"])].append(float(r["ms"]))
    if t2:
        lines += ["", "T2 CONDITION CACHE DIAGNOSTIC (ClickHouse only), median ms over the window grid"]
        for (e, arm), v in sorted(t2.items()):
            lines.append("  %-15s %-10s %s (n=%d)" % (e, arm, fmt(statistics.median(v)), len(v)))
    if loads:
        lines += ["", "L1 LOAD, INDEX/LAYOUT, SETTLE (seconds) AND STORED SIZE (bytes); one observation per engine and scenario"]
        for d in loads:
            lines.append("  %-8s %-15s load=%-9s index=%-9s settle=%-8s bytes=%s" % (
                d["scenario"], d["engine"], d["load_s"], d["index_s"], d["settle_s"], d["stored_bytes"]))
    failures = [r for r in rows if r["status"] != "ok"]
    lines += ["", "FAILURES AND EXCLUSIONS: %d of %d recorded executions" % (len(failures), len(rows))]
    kinds = defaultdict(int)
    for r in failures:
        kinds[(r["engine"], r["scenario"], r["test"], r["status"][:40])] += 1
    lines += ["  %s x%d" % (" ".join(k), n) for k, n in sorted(kinds.items())]
    lines += ["", "LIMITS",
              "  Selected analytical workloads on one machine; conclusions apply to this data, hardware and configuration.",
              "  Memory is the sampled peak of anonymous plus shared container memory (20 ms polling, a lower bound, page cache excluded).",
              "  ClickHouse query_memory is its own tracker and is not comparable to the other engines' figures.",
              "  First-run is the first execution after preparation or restart, not hardware-cold. OS caches were not evicted.",
              "  Each round restarts the server container on its persistent loaded volume; load is not repeated per round.",
              "  Row stores use B-tree indexes on created_at and customer_id; ClickHouse is sorted by (created_at, id).",
              "  The raw observations are in results.csv."]
    lines += ["", META_MARK, json.dumps({"env": env, "loads": loads}, sort_keys=True)]
    Path(txt_path).write_text("\n".join(lines) + "\n")


def merge_ch_memory(csv_path, chmem):
    """Fill ClickHouse query memory and rows read into the raw results file."""
    rows = load_results(csv_path)
    for r in rows:
        v = chmem.get((r["round"], r["position"], r["engine"], r["scenario"], r["test"], r["phase"], r["run"]))
        if v:
            r["ch_query_memory"], r["ch_read_rows"] = v
    with Path(csv_path).open("w", newline="") as f:
        w = csv.DictWriter(f, FIELDS)
        w.writeheader()
        w.writerows(rows)


def cmd_report(args):
    text = Path(args.txt).read_text()
    if META_MARK not in text:
        raise SystemExit("no metadata block in " + args.txt)
    meta = json.loads(text.split(META_MARK + "\n", 1)[1].strip().splitlines()[0])
    write_report(args.csv, meta["loads"], meta["env"], args.txt)
    print("wrote", args.txt)


# ---------------------------------------------------------------- orchestration

def cmd_generate(args):
    s = Scenario(args.rows, args.customers, args.selectivity_ppm, args.seed, args.skew, args.text_profile)
    print(json.dumps(generate(args.destination, s), indent=2))
    return 0


def cmd_plan(_args):
    print(json.dumps({"kind": "planned execution; not measured", "schedule": schedule()}, indent=2))
    return 0


def cmd_run(args):
    cores = topology()
    sudo = [] if os.geteuid() == 0 else ["sudo", "-n"]
    podman = Podman(sudo + ["podman"])
    if not args.server_cpus:
        args.server_cpus = cpuset(cores[:4])
    if not args.client_cpus:
        args.client_cpus = cpuset(cores[4:6])
    os.sched_setaffinity(0, {int(c) for c in args.client_cpus.split(",")})
    env = environment(podman, args, cores)
    if "performance" not in env["governor"]:
        print("WARNING: CPU governor is %s, not performance" % env["governor"], file=sys.stderr)
    work = Path(args.work)
    work.mkdir(parents=True, exist_ok=True)
    datadir = work / "data"
    if datadir.exists():
        raise SystemExit("work data directory exists, remove it first: %s" % datadir)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    plan = scenario_plan(args.rows)
    leftover = podman("ps", "-a", "--format", "{{.Names}}").stdout.split()
    if leftover:
        raise SystemExit("podman already has containers, refusing to share the machine: %s" % leftover)

    print("pulling images", flush=True)
    env["images"] = {}
    for name, image in IMAGES.items():
        podman("pull", "-q", image)
        env["images"][name] = image + " " + podman("image", "inspect", "-f", "{{.Digest}}", image).stdout.strip()

    print("generating fixtures", flush=True)
    expected = {}
    t2expected = {}
    for name, (s, _) in plan.items():
        generate(datadir / name, s)
        if args.rows <= 2000000:
            verify(datadir / name)
        expected[name] = json.loads((datadir / name / "expected.json").read_text())
        if name == "c1k":
            t2expected[name] = t2_oracle(datadir / name / "orders.csv", t2_windows(None))
    for p in datadir.rglob("*"):
        p.chmod(0o755 if p.is_dir() else 0o644)
    datadir.chmod(0o755)

    loads, chmem, volumes = [], {}, {n: "chr_vol_" + n for n in ENGINES}
    csv_path = out / "results.csv"
    try:
        for n in ENGINES:
            podman("volume", "create", volumes[n])
        for n in ENGINES:
            print("loading", n, flush=True)
            srv = Server(podman, n, args, volumes[n], datadir)
            srv.start()
            try:
                wait_ready(srv)
                for sc in plan:
                    d = load_scenario(srv, n, sc, sc, plan[sc][0].rows)
                    loads.append(d)
                    print("  ", d, flush=True)
            finally:
                srv.stop()
        with csv_path.open("w", newline="") as f:
            w = csv.DictWriter(f, FIELDS)
            w.writeheader()

            def writer(row):
                w.writerow({k: row.get(k, "") for k in FIELDS})

            order_rng = random.Random(1177)
            for rd in schedule(args.rounds):
                cells = [(sc, t) for sc, (_, tests) in plan.items() for t in tests]
                order_rng.shuffle(cells)
                for pos, n in enumerate(rd["engines"], 1):
                    print("round", rd["round"], "position", pos, n, flush=True)
                    srv = Server(podman, n, args, volumes[n], datadir)
                    srv.start()
                    clients = {}
                    pending = {}
                    try:
                        wait_ready(srv)
                        qbase = "r%d-p%d" % (rd["round"], pos)
                        for sc, t in cells:
                            if sc not in clients:
                                c = {"clickhouse": ClickHouseClient, "postgres": PostgresClient,
                                     "mariadb": MariaClient}[srv.family](PORTS[n], "s_" + sc)
                                clients[sc] = c
                                if srv.family == "clickhouse" and rd["round"] == 1 and sc == "c1k":
                                    env.setdefault("ch_settings", {})[n] = verify_ch_settings(c)
                            h = canonical_hash(expected[sc][t])
                            results = run_cell(srv, clients[sc], sc, t, h, rd["round"], pos, args, writer, qbase)
                            if srv.family == "clickhouse":
                                for qid, phase, i, r in results:
                                    pending[qid] = (sc, t, phase, i)
                        if srv.family == "clickhouse":
                            run_t2(srv, clients["c1k"], "c1k", t2expected["c1k"], rd["round"], pos, args, writer, qbase)
                            f.flush()
                            # Memory and rows read come from the query log; merged into results.csv at the end.
                            lg = {qid: {} for qid in pending}
                            ch_log_backfill(clients["c1k"], lg)
                            for qid, d in lg.items():
                                sc_, t_, phase_, i_ = pending[qid]
                                chmem[(str(rd["round"]), str(pos), n, sc_, t_, phase_, str(i_))] = (
                                    d.get("ch_query_memory", ""), d.get("ch_read_rows", ""))
                        f.flush()
                    finally:
                        for c in clients.values():
                            try:
                                c.close()
                            except Exception:
                                pass
                        srv.stop()
        env["finished_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        merge_ch_memory(csv_path, chmem)
        write_report(csv_path, loads, env, out / "results.txt")
        print("done: results in", out)
    finally:
        for n in ENGINES:
            podman("rm", "-f", "chr_" + n, check=False)
            podman("volume", "rm", "-f", volumes[n], check=False)
        shutil.rmtree(datadir, ignore_errors=True)
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("selftest")
    sub.add_parser("plan")
    g = sub.add_parser("generate")
    g.add_argument("destination")
    g.add_argument("--rows", type=int, default=100000)
    g.add_argument("--customers", type=int, default=1000)
    g.add_argument("--selectivity-ppm", type=int, default=10000)
    g.add_argument("--seed", type=int, default=1177)
    g.add_argument("--skew", action="store_true")
    g.add_argument("--text-profile", choices=("mixed", "random", "repetitive"), default="mixed")
    r = sub.add_parser("run")
    r.add_argument("--rows", type=int, required=True)
    r.add_argument("--out", default=str(HERE))
    r.add_argument("--work", default="/var/tmp/clickhouse-review")
    r.add_argument("--rounds", type=int, default=8)
    r.add_argument("--warmups", type=int, default=2)
    r.add_argument("--measured", type=int, default=10)
    r.add_argument("--server-cpus", default="")
    r.add_argument("--client-cpus", default="")
    r.add_argument("--server-mem", default="8g")
    rp = sub.add_parser("report")
    rp.add_argument("--csv", default=str(HERE / "results.csv"))
    rp.add_argument("--txt", default=str(HERE / "results.txt"))
    args = p.parse_args()
    return {"selftest": cmd_selftest, "plan": cmd_plan, "generate": cmd_generate,
            "run": cmd_run, "report": cmd_report}[args.command](args) or 0


if __name__ == "__main__":
    sys.exit(main())
