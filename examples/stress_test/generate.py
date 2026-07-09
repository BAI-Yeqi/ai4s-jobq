# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Generate workflow definitions for stress testing.

Creates a mix of DAG topologies (linear, diamond, wide fan-out, deep tree)
and writes them as JSON files ready for submission.

Usage::

    python generate.py --count 2000 --out-dir workflows/
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

SleepRange = tuple[float, float]

# Default sleep ranges per task role. Callers can override with a single
# (lo, hi) tuple that applies to every task in the workflow.
_DEFAULT_SLEEPS: dict[str, SleepRange] = {
    "linear": (1.0, 5.0),
    "diamond_root": (0.5, 2.0),
    "diamond_branch": (1.0, 6.0),
    "diamond_merge": (0.5, 2.0),
    "wide": (1.0, 8.0),
    "staircase_fan": (0.5, 1.5),
    "staircase_branch": (1.0, 5.0),
    "staircase_merge": (0.5, 1.5),
}


def _sleep(role: str, override: SleepRange | None) -> float:
    lo, hi = override if override is not None else _DEFAULT_SLEEPS[role]
    return random.uniform(lo, hi)


def _linear(n: int, prefix: str = "", sleep_range: SleepRange | None = None) -> list[dict]:
    """Chain of n sequential tasks."""
    tasks = []
    for i in range(n):
        t: dict = {
            "name": f"{prefix}step-{i}",
            "kwargs": {"sleep_s": _sleep("linear", sleep_range)},
        }
        if i > 0:
            t["depends_on"] = [f"{prefix}step-{i - 1}"]
        tasks.append(t)
    return tasks


def _diamond(width: int, prefix: str = "", sleep_range: SleepRange | None = None) -> list[dict]:
    """Fan-out from root → width parallel tasks → single merge task."""
    root: dict = {
        "name": f"{prefix}root",
        "kwargs": {"sleep_s": _sleep("diamond_root", sleep_range)},
    }
    middle = [
        {
            "name": f"{prefix}branch-{i}",
            "depends_on": [f"{prefix}root"],
            "kwargs": {"sleep_s": _sleep("diamond_branch", sleep_range)},
        }
        for i in range(width)
    ]
    merge: dict = {
        "name": f"{prefix}merge",
        "depends_on": [f"{prefix}branch-{i}" for i in range(width)],
        "kwargs": {"sleep_s": _sleep("diamond_merge", sleep_range)},
    }
    return [root, *middle, merge]


def _wide_parallel(n: int, prefix: str = "", sleep_range: SleepRange | None = None) -> list[dict]:
    """n independent tasks with no dependencies (embarrassingly parallel)."""
    return [
        {"name": f"{prefix}task-{i}", "kwargs": {"sleep_s": _sleep("wide", sleep_range)}}
        for i in range(n)
    ]


def _staircase(
    width: int, depth: int, prefix: str = "", sleep_range: SleepRange | None = None
) -> list[dict]:
    """Multi-layer diamond: each layer fans out then merges before the next.

    Produces depth layers of fan-out/fan-in with given width per layer.
    Total tasks = depth * (width + 1) + 1 (final merge).
    """
    tasks: list[dict] = []
    prev_merge: str | None = None

    for layer in range(depth):
        lp = f"{prefix}L{layer}-"
        fan_root = f"{lp}fan"
        fan: dict = {"name": fan_root, "kwargs": {"sleep_s": _sleep("staircase_fan", sleep_range)}}
        if prev_merge:
            fan["depends_on"] = [prev_merge]
        tasks.append(fan)

        branches = []
        for i in range(width):
            b_name = f"{lp}b{i}"
            branches.append(b_name)
            tasks.append(
                {
                    "name": b_name,
                    "depends_on": [fan_root],
                    "kwargs": {"sleep_s": _sleep("staircase_branch", sleep_range)},
                }
            )

        merge_name = f"{lp}merge"
        tasks.append(
            {
                "name": merge_name,
                "depends_on": branches,
                "kwargs": {"sleep_s": _sleep("staircase_merge", sleep_range)},
            }
        )
        prev_merge = merge_name

    return tasks


def _resilient_diamond(
    width: int, prefix: str = "", sleep_range: SleepRange | None = None
) -> list[dict]:
    """Diamond where the merge tolerates branch failures as long as one succeeds.

    Uses ``dep_policy='all_settled'`` so the merge waits for every
    branch to finish before activating (no late-result timeline) and
    activates iff at least one branch succeeded.
    """
    tasks = _diamond(width, prefix, sleep_range=sleep_range)
    tasks[-1]["dep_policy"] = "all_settled"
    for t in tasks[1:-1]:
        if random.random() < 0.3:
            t["kwargs"]["fail_probability"] = 0.5
    return tasks


# -- topology registry --
#
# Each entry is a callable taking (fan_in_width, sleep_range) and returning
# the task list. ``fan_in_width`` is honored by topologies that have a notion
# of fan-out width; topologies that don't ignore it.

TopologyFn = "callable[[int | None, SleepRange | None], list[dict]]"


def _topo_linear(fan_in_width: int | None, sleep_range: SleepRange | None) -> list[dict]:
    # ``fan_in_width`` is irrelevant for a chain; keep length random.
    return _linear(random.randint(3, 15), sleep_range=sleep_range)


def _topo_diamond(fan_in_width: int | None, sleep_range: SleepRange | None) -> list[dict]:
    width = fan_in_width if fan_in_width is not None else random.randint(3, 20)
    return _diamond(width, sleep_range=sleep_range)


def _topo_wide(fan_in_width: int | None, sleep_range: SleepRange | None) -> list[dict]:
    width = fan_in_width if fan_in_width is not None else random.randint(10, 80)
    return _wide_parallel(width, sleep_range=sleep_range)


def _topo_staircase(fan_in_width: int | None, sleep_range: SleepRange | None) -> list[dict]:
    width = fan_in_width if fan_in_width is not None else random.randint(2, 5)
    return _staircase(random.randint(2, 6), width, sleep_range=sleep_range)


def _topo_resilient_diamond(fan_in_width: int | None, sleep_range: SleepRange | None) -> list[dict]:
    width = fan_in_width if fan_in_width is not None else random.randint(4, 12)
    return _resilient_diamond(width, sleep_range=sleep_range)


TOPOLOGIES = {
    "linear": _topo_linear,
    "diamond": _topo_diamond,
    "wide": _topo_wide,
    "staircase": _topo_staircase,
    "resilient_diamond": _topo_resilient_diamond,
}

# Weighted distribution — more diamonds and staircases to stress dep tracking
WEIGHTS = {
    "linear": 15,
    "diamond": 30,
    "wide": 15,
    "staircase": 25,
    "resilient_diamond": 15,
}


def generate_workflow(
    index: int,
    *,
    topology: str | None = None,
    fan_in_width: int | None = None,
    sleep_range: SleepRange | None = None,
) -> dict:
    """Generate a single workflow definition dict.

    Parameters
    ----------
    index:
        Index used for the workflow name.
    topology:
        One of ``TOPOLOGIES`` keys to force a specific topology. ``None``
        picks randomly via the weighted distribution.
    fan_in_width:
        Override the fan-out / branch width for topologies that have one.
        Topologies without a width concept (``linear``) ignore this.
    sleep_range:
        ``(lo, hi)`` tuple applied to every task's ``sleep_s`` kwarg. When
        ``None``, each role uses its built-in default range.
    """
    if topology is not None:
        if topology not in TOPOLOGIES:
            raise ValueError(f"Unknown topology {topology!r}; valid: {sorted(TOPOLOGIES)}")
        topo = topology
    else:
        topo = random.choices(list(TOPOLOGIES.keys()), weights=list(WEIGHTS.values()), k=1)[0]
    tasks = TOPOLOGIES[topo](fan_in_width, sleep_range)
    return {
        "name": f"stress-{topo}-{index:05d}",
        "default_queue": "stress-test",
        "default_task_timeout_s": 120,
        "tasks": tasks,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate stress test workflow definitions")
    parser.add_argument("--count", type=int, default=2000, help="Number of workflows to generate")
    parser.add_argument("--out-dir", type=str, default="workflows", help="Output directory")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    args = parser.parse_args()

    random.seed(args.seed)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    total_tasks = 0
    topo_counts: dict[str, int] = {}
    flaky_tasks = 0
    expected_failed_tasks = 0.0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TextColumn("{task.fields[info]}"),
    ) as progress:
        task_id = progress.add_task("Generating", total=args.count, info="")

        for i in range(args.count):
            wf = generate_workflow(i)
            total_tasks += len(wf["tasks"])
            topo = wf["name"].split("-")[1]
            topo_counts[topo] = topo_counts.get(topo, 0) + 1
            for t in wf["tasks"]:
                p = float(t.get("kwargs", {}).get("fail_probability", 0.0))
                if p > 0:
                    flaky_tasks += 1
                    expected_failed_tasks += p
            path = out / f"wf-{i:05d}.json"
            with open(path, "w") as f:
                json.dump(wf, f)
            progress.update(
                task_id,
                advance=1,
                info=f"{total_tasks} tasks, avg {total_tasks / (i + 1):.1f}/wf",
            )

    print(f"\nGenerated {args.count} workflows ({total_tasks} total tasks) in {out}/")
    print(f"  Average tasks per workflow: {total_tasks / args.count:.1f}")
    for topo, count in sorted(topo_counts.items()):
        print(f"  {topo}: {count}")
    if flaky_tasks:
        print(
            f"  Flaky tasks: {flaky_tasks} "
            f"(expected ~{expected_failed_tasks:.0f} task failure(s); "
            f"resilient_diamond uses dep_policy='all_settled' so workflows "
            f"rarely fail as a whole)"
        )


if __name__ == "__main__":
    main()
