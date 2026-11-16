import argparse
import dataclasses
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import tempfile


@dataclasses.dataclass(frozen=True)
class Scenario:
    name: str
    items: int = 100000
    work: int = 4096
    chunk: int = 1024
    workers: int = 4
    workload: str = "ARITHMETIC"
    input: str = "GENERATED"
    storage: str = "MEMORY"
    waiting: str = "BLOCKING"
    ring: int = 1024
    heap: int = 512


SCENARIOS = [
    Scenario("cpu_light", work=64),
    Scenario("cpu_workers1", workers=1),
    Scenario("cpu_workers4"),
    Scenario("cpu_workers8", workers=8),
    Scenario("small_chunks", items=50000, chunk=32),
    Scenario("busy_spin", waiting="BUSY_SPIN"),
    Scenario("sha256", items=50000, work=8, workload="SHA256"),
    Scenario("sha256_small_heap", items=50000, work=8, workload="SHA256", heap=128),
    Scenario("csv_input", work=64, input="CSV"),
    Scenario("file_output", items=20000, work=64, chunk=128, storage="FILE"),
    Scenario("file_input_output", items=20000, work=64, input="CSV", storage="FILE"),
]
MODES = ["SPRING", "SPRING_PARALLEL", "DISRUPTOR"]


def main():
    parser = argparse.ArgumentParser(description="Compare complete Spring Batch jobs across workload and resource conditions.")
    default_java = str(Path(os.environ["JAVA_HOME"]) / "bin/java") if "JAVA_HOME" in os.environ else shutil.which("java")
    parser.add_argument("--java", default=default_java)
    parser.add_argument("--lib", type=Path, default=Path("build/batch-comparison/lib"))
    parser.add_argument("--output", type=Path, default=Path("build/reports/batch-environments"))
    parser.add_argument("--forks", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=5)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--scenarios", help="Comma-separated scenario names; default: all")
    parser.add_argument("--quick", action="store_true", help="Check execution only with 257 rows, one fork and one job")
    args = parser.parse_args()
    if not args.java or not 1 <= args.forks <= 10 or not 0 <= args.warmups <= 10 or not 1 <= args.runs <= 20:
        parser.error("Require a Java executable, forks 1..10, warmups 0..10 and runs 1..20")
    requested = set(args.scenarios.split(",")) if args.scenarios else {s.name for s in SCENARIOS}
    if requested - {s.name for s in SCENARIOS}:
        parser.error("Unknown scenarios: " + ", ".join(sorted(requested - {s.name for s in SCENARIOS})))
    scenarios = [s for s in SCENARIOS if s.name in requested]
    if args.quick:
        args.forks, args.warmups, args.runs = 1, 0, 1
        scenarios = [dataclasses.replace(s, items=257, work=min(s.work, 8)) for s in scenarios]
    lib = args.lib.resolve()
    output = args.output.resolve()
    if not list(lib.glob("*batch-compare*.jar")):
        parser.error("Build batchComparisonDistribution before running the matrix")
    output.mkdir(parents=True, exist_ok=True)
    if (output / "environment.json").exists():
        parser.error("Choose a new output directory to avoid mixing measurements")
    version = subprocess.run([args.java, "-version"], capture_output=True, text=True, check=True)
    commit = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True)
    environment = {
        "platform": platform.platform(), "java": version.stderr + version.stdout,
        "sourceCommit": commit.stdout.strip(), "quick": args.quick, "forks": args.forks,
        "warmups": args.warmups, "runs": args.runs,
        "scenarios": [dataclasses.asdict(s) for s in scenarios],
        "jars": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(lib.glob("*.jar"))},
    }
    (output / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
    results, failures = {}, []
    for repetition in range(1, args.forks + 1):
        ordered = scenarios if repetition % 2 else scenarios[::-1]
        offset = (repetition - 1) % len(MODES)
        modes = MODES[offset:] + MODES[:offset]
        for scenario in ordered:
            for mode in modes:
                name = f"{scenario.name}-{repetition}-{mode}"
                destination = output / (name + ".json")
                workers = 1 if mode == "SPRING" else scenario.workers
                with tempfile.TemporaryDirectory(prefix="batch-matrix-") as scratch:
                    command = [
                        args.java, "-da", f"-Xms{scenario.heap}m", f"-Xmx{scenario.heap}m",
                        "-Djava.io.tmpdir=" + scratch, "-cp", str(lib / "*"),
                        "com.lmax.disruptor.batchcompare.SpringBatchComparison", mode,
                        str(scenario.items), str(scenario.work), str(scenario.chunk), str(args.warmups),
                        str(args.runs), str(destination), str(workers), scenario.workload,
                        scenario.input, scenario.storage, scenario.waiting, str(scenario.ring),
                    ]
                    with (output / "commands.jsonl").open("a") as commands:
                        commands.write(json.dumps(command) + "\n")
                    with (output / (name + ".log")).open("w") as log:
                        try:
                            process = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=240)
                            if process.returncode:
                                raise RuntimeError(f"Java exited with {process.returncode}")
                        except (subprocess.TimeoutExpired, RuntimeError) as error:
                            failures.append({"scenario": scenario.name, "mode": mode, "fork": repetition, "error": str(error)})
                            print(name, "FAILED", str(error), flush=True)
                            continue
                result = json.loads(destination.read_text())
                assert result["workers"] == workers and result["workload"] == scenario.workload
                assert result["input"] == scenario.input and result["storage"] == scenario.storage
                assert len(result["samples"]) == args.runs
                assert all(s["rows"] == scenario.items and s["rollbacks"] == 0 for s in result["samples"])
                assert len({s["checksum"] for s in result["samples"]}) == 1
                results[(scenario.name, repetition, mode)] = result
                elapsed = statistics.median(s["seconds"] for s in result["samples"])
                print(name, f"{elapsed:.4f}s", flush=True)
    summary = []
    for scenario in scenarios:
        row = {"scenario": dataclasses.asdict(scenario), "modes": {}}
        for mode in MODES:
            forks = [results[(scenario.name, n, mode)] for n in range(1, args.forks + 1)
                     if (scenario.name, n, mode) in results]
            if not forks:
                continue
            times = [statistics.median(s["seconds"] for s in r["samples"]) for r in forks]
            cpus = [statistics.median(s["cpuSeconds"] for s in r["samples"]) for r in forks]
            collections = [statistics.median(s["gcCollections"] for s in r["samples"]) for r in forks]
            gc_times = [statistics.median(s["gcMillis"] for s in r["samples"]) for r in forks]
            row["modes"][mode] = {
                "forks": len(forks), "workers": forks[0]["workers"], "medianSeconds": statistics.median(times),
                "medianCpuSeconds": statistics.median(cpus), "minForkSeconds": min(times),
                "maxForkSeconds": max(times), "forkSeconds": times,
                "os": forks[0]["os"], "arch": forks[0]["arch"],
                "availableProcessors": forks[0]["availableProcessors"], "maxHeapBytes": forks[0]["maxHeapBytes"],
                "medianGcCollections": statistics.median(collections), "medianGcMillis": statistics.median(gc_times),
            }
        for n in range(1, args.forks + 1):
            peers = [results[(scenario.name, n, mode)] for mode in MODES if (scenario.name, n, mode) in results]
            assert len({s["checksum"] for r in peers for s in r["samples"]}) <= 1
            assert len({s["commits"] for r in peers for s in r["samples"]}) <= 1
        summary.append(row)
    (output / "summary.json").write_text(json.dumps({"quick": args.quick, "results": summary, "failures": failures}, indent=2) + "\n")
    print("Completed", len(results), "JVM runs; failures:", len(failures), flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
