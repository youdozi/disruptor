import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess


SCENARIOS = [
    dict(name="point_single", contract="POINT", items=1000000, work=64, workers=1, capacity=1024, keys=64, workload="ARITHMETIC"),
    dict(name="point_parallel", contract="POINT", items=1000000, work=64, workers=4, capacity=1024, keys=64, workload="ARITHMETIC"),
    dict(name="point_heavy", contract="POINT", items=100000, work=4096, workers=4, capacity=1024, keys=64, workload="ARITHMETIC"),
    dict(name="point_small_buffer", contract="POINT", items=100000, work=64, workers=4, capacity=32, keys=64, workload="ARITHMETIC"),
    dict(name="keyed_uniform", contract="KEYED", items=1000000, work=64, workers=4, capacity=1024, keys=64, workload="ARITHMETIC"),
    dict(name="keyed_heavy", contract="KEYED", items=100000, work=4096, workers=4, capacity=1024, keys=64, workload="ARITHMETIC"),
    dict(name="keyed_hot", contract="KEYED", items=1000000, work=64, workers=4, capacity=1024, keys=1, workload="ARITHMETIC"),
    dict(name="keyed_sha256", contract="KEYED", items=100000, work=8, workers=4, capacity=1024, keys=64, workload="SHA256"),
]


def main():
    parser = argparse.ArgumentParser(description="Compare bounded asynchronous messaging with matching delivery contracts.")
    parser.add_argument("--java", default=str(Path(os.environ["JAVA_HOME"]) / "bin/java") if "JAVA_HOME" in os.environ else shutil.which("java"))
    parser.add_argument("--lib", type=Path, default=Path("build/batch-comparison/lib"))
    parser.add_argument("--output", type=Path, default=Path("build/reports/channel-comparison"))
    parser.add_argument("--forks", type=int, default=3)
    parser.add_argument("--quick", action="store_true")
    args = parser.parse_args()
    if not args.java or not 1 <= args.forks <= 10:
        parser.error("Require Java and forks 1..10")
    lib, output = args.lib.resolve(), args.output.resolve()
    if not list(lib.glob("*batch-compare*.jar")):
        parser.error("Build batchComparisonDistribution first")
    output.mkdir(parents=True, exist_ok=True)
    if (output / "environment.json").exists():
        parser.error("Use a new output directory")
    scenarios = [dict(s, items=257, work=min(s["work"], 8)) for s in SCENARIOS] if args.quick else SCENARIOS
    forks, warmups, runs = (1, 0, 1) if args.quick else (args.forks, 5, 3)
    version = subprocess.run([args.java, "-version"], capture_output=True, text=True, check=True)
    commit = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True)
    environment = dict(platform=platform.platform(), java=version.stderr + version.stdout, sourceCommit=commit.stdout.strip(),
                       forks=forks, warmups=warmups, runs=runs, quick=args.quick, scenarios=scenarios,
                       jars={p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(lib.glob("*.jar"))})
    (output / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
    results, failures = {}, []
    for repetition in range(1, forks + 1):
        for scenario in (scenarios if repetition % 2 else scenarios[::-1]):
            modes = ["EXECUTOR", "QUEUE", "DISRUPTOR"] if scenario["contract"] == "POINT" else ["PARTITIONED", "DISRUPTOR"]
            offset = (repetition - 1) % len(modes)
            for mode in modes[offset:] + modes[:offset]:
                name = f'{scenario["name"]}-{repetition}-{mode}'
                destination = output / (name + ".json")
                command = [args.java, "-da", "-Xms256m", "-Xmx256m", "-cp", str(lib / "*"),
                           "com.lmax.disruptor.batchcompare.ChannelComparison", mode, scenario["contract"],
                           str(scenario["items"]), str(scenario["work"]), scenario["workload"], str(scenario["workers"]),
                           str(scenario["capacity"]), str(scenario["keys"]), str(warmups), str(runs), str(destination)]
                with (output / "commands.jsonl").open("a") as log:
                    log.write(json.dumps(command) + "\n")
                with (output / (name + ".log")).open("w") as log:
                    try:
                        process = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=240)
                        if process.returncode:
                            raise RuntimeError(f"Java exited with {process.returncode}")
                    except (subprocess.TimeoutExpired, RuntimeError) as error:
                        failures.append(dict(scenario=scenario["name"], fork=repetition, mode=mode, error=str(error)))
                        print(name, "FAILED", str(error), flush=True)
                        continue
                result = json.loads(destination.read_text())
                assert result["springIntegration"] == "7.1.2" and result["mode"] == mode and result["contract"] == scenario["contract"]
                assert len(result["samples"]) == runs
                assert all(s["completed"] == scenario["items"] and 1 <= s["activeWorkers"] <= scenario["workers"] for s in result["samples"])
                results[(scenario["name"], repetition, mode)] = result
                print(name, "events/s", round(statistics.median(s["eventsPerSecond"] for s in result["samples"])),
                      "p99(us)", round(statistics.median(s["p99Micros"] for s in result["samples"]), 2), flush=True)
    summary = []
    for scenario in scenarios:
        row = dict(scenario=scenario, modes={})
        modes = ["EXECUTOR", "QUEUE", "DISRUPTOR"] if scenario["contract"] == "POINT" else ["PARTITIONED", "DISRUPTOR"]
        for mode in modes:
            values = [results[(scenario["name"], n, mode)] for n in range(1, forks + 1) if (scenario["name"], n, mode) in results]
            if not values:
                continue
            data = {}
            for key in ["seconds", "eventsPerSecond", "cpuSeconds", "p99Micros", "p999Micros", "activeWorkers"]:
                per_fork = [statistics.median(s[key] for s in v["samples"]) for v in values]
                data[key] = dict(median=statistics.median(per_fork), minimum=min(per_fork), maximum=max(per_fork), forks=per_fork)
            row["modes"][mode] = dict(forks=len(values), os=values[0]["os"], arch=values[0]["arch"],
                                     availableProcessors=values[0]["availableProcessors"], metrics=data)
        summary.append(row)
    (output / "summary.json").write_text(json.dumps(dict(quick=args.quick, results=summary, failures=failures), indent=2) + "\n")
    print("Completed", len(results), "JVM runs; failures:", len(failures), flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
