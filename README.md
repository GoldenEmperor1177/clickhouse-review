# ClickHouse 26.10 review

ClickHouse 26.10 brings a long list of new features and performance improvements. This project looks at a smaller, practical question: how much difference does upgrading make to the analytical queries we run?

We compare ClickHouse 26.10 with 26.9, with PostgreSQL and MariaDB using InnoDB as additional baselines. The review covers query performance, memory use, loading costs and correctness. The queries, configuration and raw measurements are published alongside the findings.

The measured run is complete at 1 million orders. All 7,872 timed executions returned the expected answer, with no errors and no timeouts.

## Findings

The numbers below come from `results.txt` and `results.csv`. Each upgrade ratio is the median over eight rounds of the per-round ratio of ClickHouse 26.10 to 26.9, so a value below 1 means 26.10 was faster. The range is the lowest and highest round.

| Test | Scenarios | 26.9 to 26.10 | Reading |
|---|---|---|---|
| Prefix filter (S1) | 3 selectivities | 0.76 to 0.80 | About 20 to 24% faster, faster in every round |
| Substring filter (S2) | 3 selectivities | 0.70 to 0.77 | About 23 to 30% faster, faster in every round |
| Join (J1) | 4 dimension and skew cases | 0.91 to 0.98 | About 2 to 9% faster, faster in every round |
| Grouped aggregation (A1) | 3 cardinalities | 0.99 to 1.02 | Unchanged within noise |
| Exact distinct (A2) | 3 cardinalities | 0.97 to 1.02 | Unchanged within noise |
| Fixed seven-day window (T1) | 1 | 1.05 | Slightly slower, 3.3 ms to 3.4 ms, near the timing noise floor |
| Control sum (C1) | 1 | 1.01 | Unchanged |

Other observations:

- Query memory for the text filters fell from 64.1 MiB to 13.8 MiB at 1% selectivity, using ClickHouse's own query tracker on both builds. This is a same-engine comparison.
- The join slowdown documented for 26.9 in DB Pairs is not tested here. This review only compares 26.9 with 26.10, so it cannot say whether that slowdown reproduces. What it shows is that 26.10 joins are slightly faster than 26.9 in this setup.
- A2 at 100,000 customers (1.02, every round above 1) and T1 (1.05, every round above 1) are small but consistent slowdowns. In absolute terms they are 0.1 to 0.2 ms.
- The moving-window condition cache diagnostic (T2) showed no difference between cache off and cache on at this scale (4.5 ms for both on 26.10). At 1 million rows these queries are too fast for the cache to matter, so this result says nothing about the feature at larger scale.
- ClickHouse stored the data in about 106 MB, PostgreSQL in about 262 MB and MariaDB in about 248 MB for the 1,000 customer scenario. Loading took under 3 seconds and index builds under 1 second on every engine. MariaDB then needed 56 to 110 seconds of forced flushing to settle.
- PostgreSQL was about 1.6 to 12 times slower than ClickHouse on these tasks, and MariaDB slower again, apart from one case where MariaDB's index made exact distinct on 1,000 customers very fast. These results give context only and do not establish which database is best for any application.

## What we tested

The review has two parts:
- Compare the two ClickHouse releases to measure the effect of upgrading.
- Compare all four database builds on equivalent analytical tasks.

| Test | What it covers |
|---|---|
| A1 grouped aggregation | Counts and revenue sums across 1,000, 100,000 and 1,000,000 customer groups |
| A2 exact distinct counts | Counting unique customers across the same cardinalities |
| S1 prefix filtering | Finding messages that start with a specified word, at 0.1%, 1% and 10% selectivity |
| S2 substring filtering | Finding a specified word within a message, at the same selectivities |
| T1 fixed time window | Aggregating a seven-day slice |
| T2 moving time window | ClickHouse condition cache off and on, with correctness checked against an independent answer |
| J1 joins | Orders joined with customers, with 1,000, 100,000 and 1,000,000 customers and with skewed keys |
| C1 control query | Summing a column without filters or joins |
| L1 loading | Loading data, preparing indexes and layouts, settling, and measuring stored size |

## Database builds

| Database | Version | Role |
|---|---|---|
| ClickHouse | 26.9.27.3 | Previous-release baseline |
| ClickHouse | 26.10.3.7 | Release under review |
| PostgreSQL | 18.6 | Row-store baseline |
| MariaDB with InnoDB | 11.8.9 | Row-store baseline |

All four ran from official container images. The image digests are recorded in `results.txt`. DuckDB-based integrations, ColumnStore and external analytical engines are outside the scope.

## Data

The suite generates a synthetic dataset containing customers and orders. Every engine receives the same logical data, and the generator is deterministic with seed 1177.

We vary customer cardinality, text-filter selectivity and customer-key skew, one factor at a time. Money is stored as integer cents, and timestamps use UTC whole seconds. Each message is 128 ASCII characters.

The text tests use a mixed service-style profile. Prefix and substring tests use separate markers with equal match counts. The customer assignment is cyclic, and the text profiles are controls, not production traces.

Scales run:
- 100,000 orders for smoke checks of the runner.
- 1 million orders for the published measurements.

The 10 million order run was planned and then dropped as excessive for this question. Nothing here should be read as a result at that scale.

## Measurements

For every execution we collect:
- Query latency through full result consumption, timed with a monotonic clock.
- A correctness check of the full result against an independently calculated answer.
- CPU time and physical read bytes from the container cgroup.
- Peak container memory, sampled every 20 ms, counting anonymous and shared memory but not page cache. This is a lower bound.
- ClickHouse query memory and rows read from its query log. This tracker is reported separately and is not comparable with the other engines.

For loading we record load time, index or layout time, settle time and stored size.

Not collected: spill information, query plans, a concurrency phase, and hardware-cold runs. First-run means the first execution after the server restart, with operating system caches left alone. We do not call any run cold.

## How we keep the comparison fair

Each server runs in a container with four physical cores (eight threads) and 8 GiB of memory with no swap, on the same core set. The client runs on two other cores. Engines run one at a time. An eight-round schedule balances each engine across execution positions and adjacent order.

The two ClickHouse releases use identical schemas, layouts and settings. Result caching is off and the condition cache is on for both, and the applied settings are read back from each server and recorded. PostgreSQL and MariaDB use native B-tree indexes. Server settings are in `bench.py`: PostgreSQL uses 2 GB shared buffers and MariaDB a 4 GB buffer pool.

Each round restarts the server container on its loaded volume, so the load is done once per engine and not repeated per round. Each cell has one first run, two warmups and ten measured runs, and the summary uses the median of the eight per-round medians. Raw observations, failures and exclusions are kept in `results.csv`. The per-query timeout is 120 seconds, and a timeout is recorded as a failure, not as a 120-second result.

## Hardware

The run used one desktop: AMD Ryzen 7 9700X (8 cores, 16 threads), 30 GB memory, NVMe storage, Fedora 44 with kernel 7.2.11 and rootful Podman 5.8.4. The CPU governor was set to performance for the run. Other services on the machine were stopped for the duration.

## Project files

| File | Contents |
|---|---|
| `bench.py` | Dataset generation, SQL, database setup, correctness checks, measurements and reporting |
| `results.csv` | Raw measurements from each engine, scenario, test, round and run |
| `results.txt` | Readable results, environment details, configuration and limitations |
| `README.md` | Scope, methodology, setup instructions and review findings |

The script generates temporary working data during execution and removes it afterwards. `results.txt` ends with a metadata block that the `report` command reads. It also records the hash of the `bench.py` that produced the measurements, which is the version in commit `5386bba`. Later commits changed only the `report` command, not data generation, queries or measurement.

## Setup and running

You need Python 3, Podman with passwordless sudo, and several GB of free disk for the container images, fixtures and volumes. We ran it on Python 3.14. Install the two Python drivers in a virtual environment:

```
python3 -m venv venv
venv/bin/pip install "psycopg[binary]" pymysql
```

Run the offline self-test, which needs no containers and takes about a second:

```
venv/bin/python bench.py selftest
```

Run the measurement. The run refuses to start if Podman already has containers, because it must not share the machine:

```
venv/bin/python bench.py run --rows 1000000 --out .
```

Useful options are `--rounds`, `--warmups`, `--measured`, `--server-cpus`, `--client-cpus` and `--server-mem`. The published run used the defaults and took about 36 minutes. A quick check of the runner is `--rows 100000 --rounds 4 --warmups 1 --measured 2`, which takes a few minutes and should not be mixed with real results. For stable timings, set the CPU governor to performance and stop other services first. `venv/bin/python bench.py report` rebuilds `results.txt` from `results.csv` in place, and regenerating the published files reproduces them byte for byte. To write elsewhere, pass `--txt OUT --meta results.txt`.

## Relationship to DB Pairs

This project follows the compact structure of DB Pairs. It carries forward its emphasis on resource budgets, correctness checks and reproducible reporting.

DB Pairs v2.1.1 remains a separate release. Historical DB Pairs measurements are not presented as results from this experiment.

## Limits of the review

This is a focused review of selected analytical workloads. It does not cover every release change, transactional application performance, replication, CDC or production operations. We did not run the two feature walkthroughs that were considered, and we did not capture query plans.

The results come from one machine at one scale, and the conclusions apply to the data, hardware and configurations we tested. Load time and stored size are single observations per engine and scenario. Differences of a few percent are within the round-to-round noise.
