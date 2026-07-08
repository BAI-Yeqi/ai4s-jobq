# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Generate a CSE-topology workflow from a sample-data JSON file.

The input describes a set of *B nodes*, each referencing a small set of
*molecules* (``M`` nodes).  The generator produces a single workflow
whose DAG mirrors the plan from ``plan.md`` §2, with the ``g()`` node
expanded into ``--sim-count`` independent sibling tasks (default 5)
so the downstream ``energy()`` gate fans in across several parallel
sub-computations:

* **m-<idx>** — one root task per unique molecule, computing
  ``baselines(M)``.  Molecules are sequentially indexed ``M1, M2, ...``
  in sorted order of their hash; the original hash is preserved in
  kwargs.
* **b-<idx>** — one gate task per B node, ``dep_policy=ALL`` on its M's.
* **sim-<k>-<m_idx>** — one task per (k, M) pair, ``dep_policy=ANY``
  on the B's that contain M.  ``ANY`` deduplicates each ``sim_k(M)``:
  it runs exactly once even when shared by multiple B's.  Within a
  molecule the five sims are independent (no edges between them).
* **d-<idx>** — one trigger task per B, computing ``energy(B)``.
  ``dep_policy=ALL`` on the B itself *plus* every ``sim-<k>-<m_idx>``
  for each M in B.  In Phase 0 these are no-op leaves; Phase 1's
  spawn-sub-workflow primitive will turn them into per-B downstream
  launches.

Run ``inspect`` for stats without writing files; ``generate`` to emit
the workflow JSON ready for ``ai4s-jobq workflow submit``.

Usage::

    python cse_workflow.py inspect --input sample-data-mindless.json

    python cse_workflow.py generate \\
        --input sample-data-mindless.json --out cse-workflow.json

    # With realistic sleep ranges (compressed by --time-scale):
    python cse_workflow.py generate --input sample-data-mindless.json \\
        --out cse-workflow.json \\
        --m-seconds 60 300 --sim-seconds 30 120 --time-scale 60

    # Tune the per-molecule sim fan-out (default 5):
    python cse_workflow.py generate --input sample-data-mindless.json \\
        --out cse-workflow.json --sim-count 3
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.table import Table

from ai4s.jobq.workflow.entities import (
    DepPolicy,
    WorkflowDefinition,
    WorkflowTask,
)

LOG = logging.getLogger("ai4s.jobq.workflow.stress.cse")


# ---------------------------------------------------------------------------
# Input model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BInput:
    """One B node from the input file."""

    index: int
    members: tuple[tuple[float, str], ...]  # list of (count, molecule_hash)


def _load_bs(path: Path) -> list[BInput]:
    raw = json.loads(path.read_text())
    if not isinstance(raw, list):
        raise ValueError(f"{path}: expected a top-level list of B nodes")

    out: list[BInput] = []
    for i, b in enumerate(raw):
        if not isinstance(b, list):
            raise ValueError(f"{path}: B[{i}] must be a list, got {type(b).__name__}")
        members: list[tuple[float, str]] = []
        for j, entry in enumerate(b):
            if not isinstance(entry, dict):
                raise ValueError(f"{path}: B[{i}][{j}] must be an object")
            try:
                members.append((float(entry["count"]), str(entry["molecule"])))
            except KeyError as exc:
                raise ValueError(
                    f"{path}: B[{i}][{j}] missing required field {exc.args[0]!r}"
                ) from exc
        out.append(BInput(index=i, members=tuple(members)))
    return out


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BatchStats:
    n_b: int
    n_m: int  # unique molecule (M) nodes
    edges: int
    fan_in: tuple[int, int, float]
    bs_per_m: tuple[int, int, float]
    sim_count: int = 5

    @property
    def n_sim(self) -> int:
        """Total sim tasks: sim_count copies per unique M."""
        return self.sim_count * self.n_m

    @property
    def n_d(self) -> int:
        return self.n_b

    @property
    def total_tasks(self) -> int:
        return self.n_m + self.n_b + self.n_sim + self.n_d


def _compute_stats(bs: list[BInput], *, sim_count: int = 5) -> BatchStats:
    if not bs:
        return BatchStats(0, 0, 0, (0, 0, 0.0), (0, 0, 0.0), sim_count=sim_count)

    fan_in_counts = [len(b.members) for b in bs]
    m_to_bs: dict[str, list[int]] = defaultdict(list)
    for b in bs:
        for _count, mol in b.members:
            m_to_bs[mol].append(b.index)
    bs_per_m_counts = [len(v) for v in m_to_bs.values()]

    return BatchStats(
        n_b=len(bs),
        n_m=len(m_to_bs),
        edges=sum(fan_in_counts),
        fan_in=(
            min(fan_in_counts),
            max(fan_in_counts),
            sum(fan_in_counts) / len(fan_in_counts),
        ),
        bs_per_m=(
            min(bs_per_m_counts),
            max(bs_per_m_counts),
            sum(bs_per_m_counts) / len(bs_per_m_counts),
        ),
        sim_count=sim_count,
    )


# ---------------------------------------------------------------------------
# DAG construction
# ---------------------------------------------------------------------------

M_PREFIX = "m-"
B_PREFIX = "b-"
SIM_PREFIX = "sim-"
D_PREFIX = "d-"

# Default number of independent sim tasks per M (expansion of the
# original single ``g(M)`` node).  Overridable via ``--sim-count``.
SIM_COUNT_DEFAULT = 5


def _b_name(index: int) -> str:
    return f"{B_PREFIX}{index:07d}"


def _d_name(index: int) -> str:
    return f"{D_PREFIX}{index:07d}"


def _m_name(index: int) -> str:
    """Sequential M index, 1-based and zero-padded to 7 digits."""
    return f"{M_PREFIX}{index:07d}"


def _sim_name(k: int, m_index: int) -> str:
    """1-based sim variant ``k`` for the molecule indexed by ``m_index``."""
    return f"{SIM_PREFIX}{k}-{m_index:07d}"


@dataclass(frozen=True)
class QueueConfig:
    """Per-role target queue names."""

    m: str = "stress-test"
    b: str = "stress-test"
    sim: str = "stress-test"
    d: str = "stress-test"


@dataclass(frozen=True)
class SleepProfile:
    """Sampled task durations (seconds) per role.

    Each range is ``(lo, hi)`` in real seconds, divided by
    ``time_scale`` to compress to wall-clock seconds for tractable
    stress runs.
    """

    m: tuple[float, float] = (0.1, 0.1)
    b: tuple[float, float] = (0.0, 0.0)
    sim: tuple[float, float] = (0.1, 0.1)
    d: tuple[float, float] = (0.0, 0.0)
    time_scale: float = 1.0

    def sample(self, role: str) -> float:
        lo, hi = getattr(self, role)
        if lo == hi == 0.0:
            return 0.0
        return max(0.0, random.uniform(lo, hi) / self.time_scale)


def build_workflow(
    bs: list[BInput],
    *,
    name: str,
    queues: QueueConfig,
    sleep: SleepProfile,
    num_retries: int = 0,
    default_task_timeout_s: int | None = None,
    max_parallelism: int | None = None,
    fail_pct_m: int = 0,
    fail_pct_other: int = 0,
    sim_count: int = SIM_COUNT_DEFAULT,
) -> WorkflowDefinition:
    """Build the CSE workflow definition from a parsed B list.

    Each unique molecule ``A`` expands into ``sim_count`` independent
    sibling tasks ``sim-1-A`` .. ``sim-{sim_count}-A``, all gated by
    ``dep_policy=ANY`` on the B's containing ``A``.  The per-B trigger
    ``d-<idx>`` depends on the B itself *plus* every sim sibling for
    each of B's molecules.

    The output passes :meth:`WorkflowDefinition.validate` (raises if
    the input produces a malformed DAG, which should not happen for
    any well-formed sample file).

    *fail_pct_m* and *fail_pct_other* are **integer percentages** (0-100)
    that control deterministic failure in ``DummyWorkflowProcessor``.
    The processor hashes the task name and compares against the
    threshold, so the same task **always** fails on every attempt —
    downstream tasks will be marked ``upstream_failed``.  Set
    *num_retries* > 0 only if you want to observe the retry exhaustion
    path before the final failure.
    """

    if sim_count < 1:
        raise ValueError(f"sim_count must be >= 1, got {sim_count}")

    m_to_bs: dict[str, list[int]] = defaultdict(list)
    for b in bs:
        for _count, mol in b.members:
            m_to_bs[mol].append(b.index)

    # Sequential 1-based index per unique molecule, sorted by hash so
    # the mapping is deterministic across runs.  ``mol_to_idx[hash]``
    # gives the integer that appears in the ``m-<idx>`` task name.
    mol_to_idx: dict[str, int] = {mol: i for i, mol in enumerate(sorted(m_to_bs), start=1)}

    tasks: list[WorkflowTask] = []

    # --- M tasks: roots, one per unique molecule, computing baselines(M) ---
    for mol, m_idx in sorted(mol_to_idx.items(), key=lambda kv: kv[1]):
        tasks.append(
            WorkflowTask(
                name=_m_name(m_idx),
                kwargs={
                    "role": "baselines",
                    "m_index": m_idx,
                    "molecule": mol,
                    "sleep_s": sleep.sample("m"),
                    **({"fail_threshold": fail_pct_m} if fail_pct_m else {}),
                },
                depends_on=[],
                queue=queues.m,
                dep_policy=DepPolicy.ALL,
                num_retries=num_retries,
            )
        )

    # --- B tasks: gates over their constituent M's ---------------------
    for b in bs:
        # dedup molecule occurrences within a degenerate B (same
        # molecule listed twice with opposite count signs); preserves
        # first-occurrence order via dict.fromkeys.
        m_names = list(dict.fromkeys(_m_name(mol_to_idx[mol]) for _count, mol in b.members))
        tasks.append(
            WorkflowTask(
                name=_b_name(b.index),
                kwargs={
                    "role": "b",
                    "members": [
                        {"count": c, "molecule": m, "m_index": mol_to_idx[m]} for c, m in b.members
                    ],
                    "sleep_s": sleep.sample("b"),
                    **({"fail_threshold": fail_pct_other} if fail_pct_other else {}),
                },
                depends_on=m_names,
                queue=queues.b,
                dep_policy=DepPolicy.ALL,
                num_retries=num_retries,
            )
        )

    # --- sim tasks: sim_count independent siblings per molecule --------
    # Each sim-k-<m_idx> has dep_policy=ANY over the B's containing
    # the molecule, deduplicating the computation across B's.  The
    # five sims for a single molecule have no edges between them; they
    # represent independent variants (replicas, settings, ...) that
    # the downstream energy() gate must wait for collectively.
    for mol, b_indices in sorted(m_to_bs.items(), key=lambda kv: mol_to_idx[kv[0]]):
        m_idx = mol_to_idx[mol]
        # Dedup B-indices (a degenerate B contributes the same index
        # multiple times) and sort for stable round-trip output.
        dep_b_names = [_b_name(i) for i in sorted(set(b_indices))]
        tasks.extend(
            WorkflowTask(
                name=_sim_name(k, m_idx),
                kwargs={
                    "role": "sim",
                    "sim_index": k,
                    "m_index": m_idx,
                    "molecule": mol,
                    "sleep_s": sleep.sample("sim"),
                    **({"fail_threshold": fail_pct_other} if fail_pct_other else {}),
                },
                depends_on=dep_b_names,
                queue=queues.sim,
                # ANY ensures sim_k(M) becomes READY the moment the
                # *first* B containing M succeeds. Subsequent satisfied
                # B's that also reference M find sim_k(M) already past
                # PENDING and are no-ops (verified: store.py:1517-1555).
                dep_policy=DepPolicy.ANY,
                num_retries=num_retries,
            )
            for k in range(1, sim_count + 1)
        )

    # --- d tasks: energy(B), gated by B and its M's sim siblings -------
    for b in bs:
        b_name = _b_name(b.index)
        # Group all sim variants per molecule, in (molecule-order, k-order)
        # for readability.  Dedup across degenerate B's that list the
        # same molecule twice.
        sim_names: list[str] = []
        seen_mols: set[str] = set()
        for _count, mol in b.members:
            if mol in seen_mols:
                continue
            seen_mols.add(mol)
            m_idx = mol_to_idx[mol]
            sim_names.extend(_sim_name(k, m_idx) for k in range(1, sim_count + 1))
        # Per critique: d(B_j) depends on B_j *itself* AND the sim
        # siblings of B_j's inputs. Without the B_j edge, energy()
        # could fire based on sims triggered by *other* B's containing
        # the same M's — leaking unrelated success into B_j's downstream.
        deps = [b_name, *sim_names]
        tasks.append(
            WorkflowTask(
                name=_d_name(b.index),
                kwargs={
                    "role": "energy",
                    "b_index": b.index,
                    "members": [
                        {"count": c, "molecule": m, "m_index": mol_to_idx[m]} for c, m in b.members
                    ],
                    "sleep_s": sleep.sample("d"),
                    **({"fail_threshold": fail_pct_other} if fail_pct_other else {}),
                },
                depends_on=deps,
                queue=queues.d,
                dep_policy=DepPolicy.ALL,
                num_retries=num_retries,
            )
        )

    definition = WorkflowDefinition(
        name=name,
        tasks=tasks,
        default_queue="stress-test",
        default_task_timeout_s=default_task_timeout_s,
        max_parallelism=max_parallelism,
    )
    definition.validate()
    return definition


# ---------------------------------------------------------------------------
# Reporting helpers
# ---------------------------------------------------------------------------


def _render_stats(stats: BatchStats, console: Console, title: str = "Batch shape") -> None:
    t = Table(title=title, show_header=True)
    t.add_column("metric")
    t.add_column("value", justify="right")
    t.add_row("B nodes", f"{stats.n_b:,}")
    t.add_row("M nodes (unique molecules)", f"{stats.n_m:,}")
    t.add_row("B→M edges", f"{stats.edges:,}")
    t.add_row(
        "B fan-in (min/avg/max)",
        f"{stats.fan_in[0]} / {stats.fan_in[2]:.2f} / {stats.fan_in[1]}",
    )
    t.add_row(
        "B's per M (min/avg/max)",
        f"{stats.bs_per_m[0]} / {stats.bs_per_m[2]:.3f} / {stats.bs_per_m[1]}",
    )
    t.add_row("sim siblings per M", f"{stats.sim_count}")
    t.add_row(
        f"sim tasks (= {stats.sim_count} x unique M's)",
        f"{stats.n_sim:,}",
    )
    t.add_row("d (energy) tasks (= B count)", f"{stats.n_d:,}")
    t.add_row("[bold]total workflow tasks[/]", f"[bold]{stats.total_tasks:,}[/]")
    console.print(t)


def _render_distribution(bs: list[BInput], console: Console) -> None:
    """Histogram of how many B's reference each M — the sim-dedup payoff."""
    m_to_bs: dict[str, list[int]] = defaultdict(list)
    for b in bs:
        for _count, mol in b.members:
            m_to_bs[mol].append(b.index)
    bucket = Counter(len(v) for v in m_to_bs.values())

    t = Table(title="sim-dedup payoff: B's per M", show_header=True)
    t.add_column("# B's per M", justify="right")
    t.add_column("# M's", justify="right")
    t.add_column("share of M's", justify="right")
    total = sum(bucket.values())
    for k in sorted(bucket):
        t.add_row(str(k), f"{bucket[k]:,}", f"{bucket[k] / total:.2%}")
    console.print(t)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_pair(s: str) -> tuple[float, float]:
    """Parse 'lo hi' or 'lo,hi' into a (float, float)."""
    parts = s.split(",") if "," in s else s.split()
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(f"expected 'lo hi' or 'lo,hi'; got {s!r}")
    return float(parts[0]), float(parts[1])


def _add_common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to the sample-data.json file (list of B nodes).",
    )


def _add_generate_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output workflow JSON path.",
    )
    p.add_argument(
        "--name",
        type=str,
        default=None,
        help="Workflow name (default: derived from --out stem).",
    )
    p.add_argument(
        "--m-queue",
        default="stress-test",
        help="Queue name for M (baselines) tasks.",
    )
    p.add_argument(
        "--b-queue",
        default="stress-test",
        help="Queue name for B (gate) tasks.",
    )
    p.add_argument(
        "--sim-queue",
        default="stress-test",
        help="Queue name for sim-k tasks.",
    )
    p.add_argument(
        "--d-queue",
        default="stress-test",
        help="Queue name for d (energy) tasks.",
    )
    p.add_argument(
        "--sim-count",
        type=int,
        default=SIM_COUNT_DEFAULT,
        help=(
            f"Number of independent sim siblings per molecule "
            f"(default {SIM_COUNT_DEFAULT}). Each sim-k-<mol> task is "
            "independent of the others."
        ),
    )
    p.add_argument(
        "--m-seconds",
        type=_parse_pair,
        default="0.1 0.1",
        help="Real-seconds range for M (baselines) tasks (e.g. '60 300'). Compressed by --time-scale.",
    )
    p.add_argument(
        "--b-seconds",
        type=_parse_pair,
        default="0 0",
        help="Real-seconds range for B (gate) tasks — typically 0 (no-op).",
    )
    p.add_argument(
        "--sim-seconds",
        type=_parse_pair,
        default="0.1 0.1",
        help="Real-seconds range for sim-k tasks.",
    )
    p.add_argument(
        "--d-seconds",
        type=_parse_pair,
        default="0 0",
        help="Real-seconds range for d (energy) tasks — typically 0.",
    )
    p.add_argument(
        "--time-scale",
        type=float,
        default=1.0,
        help="Divisor applied to sampled seconds (e.g. 60 = 1 real-minute → 1 wall-second).",
    )
    p.add_argument(
        "--num-retries",
        type=int,
        default=0,
        help="num_retries on every task. Default 0 = single attempt.",
    )
    p.add_argument(
        "--default-task-timeout-s",
        type=int,
        default=None,
        help="default_task_timeout_s on the workflow.",
    )
    p.add_argument(
        "--max-parallelism",
        type=int,
        default=None,
        help="Cap on in-flight tasks per workflow (max_parallelism).",
    )
    p.add_argument(
        "--fail-m",
        type=int,
        default=0,
        metavar="PCT",
        help=(
            "Deterministic failure rate (integer %%) for M (baselines) tasks — every attempt "
            "fails.  Tasks whose SHA-256 name-hash %% 100 < PCT always fail, causing downstream "
            "cascade to upstream_failed.  Default 0 (no failures)."
        ),
    )
    p.add_argument(
        "--fail-other",
        type=int,
        default=0,
        metavar="PCT",
        help=(
            "Deterministic failure rate (integer %%) for B, sim, and d tasks — every attempt "
            "fails.  Same hash-based rule as --fail-m.  Default 0."
        ),
    )
    p.add_argument(
        "--seed",
        type=int,
        default=42,
        help="RNG seed for sleep_s sampling. Default 42.",
    )
    p.add_argument(
        "--show-stats",
        action="store_true",
        help="Print batch shape + sim-dedup distribution after generation.",
    )


def cmd_inspect(args: argparse.Namespace) -> int:
    console = Console()
    bs = _load_bs(args.input)
    stats = _compute_stats(bs, sim_count=args.sim_count)
    _render_stats(stats, console, title=f"Batch shape: {args.input}")
    _render_distribution(bs, console)
    return 0


def cmd_generate(args: argparse.Namespace) -> int:
    console = Console()
    bs = _load_bs(args.input)
    random.seed(args.seed)

    name = args.name or args.out.stem
    queues = QueueConfig(m=args.m_queue, b=args.b_queue, sim=args.sim_queue, d=args.d_queue)
    sleep = SleepProfile(
        m=args.m_seconds,
        b=args.b_seconds,
        sim=args.sim_seconds,
        d=args.d_seconds,
        time_scale=args.time_scale,
    )
    if args.fail_m < 0 or args.fail_m > 100:
        raise ValueError("--fail-m must be in [0, 100]")
    if args.fail_other < 0 or args.fail_other > 100:
        raise ValueError("--fail-other must be in [0, 100]")

    definition = build_workflow(
        bs,
        name=name,
        queues=queues,
        sleep=sleep,
        num_retries=args.num_retries,
        default_task_timeout_s=args.default_task_timeout_s,
        max_parallelism=args.max_parallelism,
        fail_pct_m=args.fail_m,
        fail_pct_other=args.fail_other,
        sim_count=args.sim_count,
    )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    payload = definition.to_json()
    args.out.write_text(payload)

    if args.show_stats:
        stats = _compute_stats(bs, sim_count=args.sim_count)
        _render_stats(stats, console, title=f"Generated: {args.out}")
        _render_distribution(bs, console)

    console.print(
        f"Wrote [cyan]{args.out}[/] — "
        f"[bold]{len(definition.tasks):,}[/] tasks, "
        f"{len(payload) / 1024 / 1024:.1f} MB JSON"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_inspect = sub.add_parser("inspect", help="Stats only; no file written.")
    _add_common_args(p_inspect)
    p_inspect.add_argument(
        "--sim-count",
        type=int,
        default=SIM_COUNT_DEFAULT,
        help=(f"Project totals for this many sim siblings per A (default {SIM_COUNT_DEFAULT})."),
    )
    p_inspect.set_defaults(func=cmd_inspect)

    p_generate = sub.add_parser("generate", help="Write a workflow JSON file.")
    _add_common_args(p_generate)
    _add_generate_args(p_generate)
    p_generate.set_defaults(func=cmd_generate)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
