# sds-finality-sim

Simulator for **SDS-FINALITY**: a fixed group of N members agrees on one total
order of MAIN messages (e.g. MLS commits) on top of SDS. A MAIN is final at
position k once more than N/2 members report it as the first MAIN they saw
outside positions 1..k-1.

The SDS layer is the **real nim-sds code**, run in-process with a virtual clock.
The finality layer follows the spec.

## Build

Needs Nim ≥ 2.2, git and python3.

```sh
cd nim-sds
sh simnim/build.sh        # fetches locked deps into ../deps, builds build/finality_sim
```

## Run

```sh
python3 simnim/run.py --n 50 --loss 10,20 --max-delay 30 --variant A,B --seeds 10
```

Comma-separated values are swept, and every combination is run once per seed.
Per configuration the runner prints, counted over runs:

- **safety:** no two nodes finalized conflicting orders.
- **complete:** every node finalized all `k` mains.
- **order:** the agreed order equals the release order.

It also prints latency, traffic, and any runs that deviated.

## Arguments

| Argument | Default | Meaning |
|---|---|---|
| `--n` | 50 | Number of nodes; quorum is N/2 + 1. |
| `--loss` | 10 | Packet loss %, independent per packet and receiver. |
| `--max-delay` | 30 | Maximum one-way delay (s); the minimum is `--min-delay` 0.05. |
| `--delay` | uniform | `uniform`; `tail` (90% of packets in the first 10% of the range); `geo` (delay correlated with node distance). |
| `--interval` | 60 | Seconds between MAIN releases (network-wide rate). |
| `--k` | 5 | Number of MAINs per run. |
| `--variant` | A | `A` = spec. `B` = A + every node that has not finalized re-sends its non-finalized mains and latest view every `--regossip` (20) s. |
| `--sync` | 30 | SYNC heartbeat period (s); `0` disables SYNC. |
| `--senders` | random | `random`: M1 from n0, the rest from random nodes. `single`: all from n0 (one steward). |
| `--payload` | 0 | MAIN payload size in bytes. |
| `--bad-frac` / `--bad-loss` | 0 / 50 | Fraction of nodes with a bad link, and the loss % on their packets. |
| `--offline-frac` / `--offline-dur` | 0 / 120 | Fraction of nodes offline for `offline-dur` s, starting right after M1. |
| `--votes` / `--admit` | received | `delivered` waits for SDS causal delivery before counting a vote or admitting a MAIN (old behaviour). |
| `--seeds` / `--seed` | 10 / 0 | Runs per configuration, and the first seed. |
| `--jobs` | #CPUs | Parallel runs. |
| `--timeout` | none | Wall-clock limit per run (s); the run stops and reports a partial result. |
| `--run-to-end` | off | Keep simulating after all nodes finalized, e.g. to measure post-finality traffic. |
| `--out` | none | Append raw per-run JSON lines to a file. |

Less common flags are listed under `python3 simnim/run.py -h` and
`./build/finality_sim --help`. With `SIM_DIAG=1`, the simulator prints the SDS
buffer state of nodes that did not finish.

Runs are deterministic: the same flags and seed give the same result on any
machine.

## Example scenarios

```sh
R="python3 simnim/run.py --timeout 600"

# Loss tolerance, spec vs re-gossip
$R --n 50 --loss 10,20,30,40 --max-delay 15 --variant A,B --seeds 10

# Scale
$R --n 5,10,20,50,100 --loss 10 --max-delay 10 --variant B --seeds 5

# Release-order fidelity when mains overlap (multi-steward race), i.i.d. vs correlated delay
$R --n 50 --loss 10 --max-delay 30 --interval 5,10,30,60 --delay uniform,geo --seeds 10

# SYNC cost
$R --n 50 --loss 10,30 --max-delay 10 --interval 90 --sync 0,30,120 --variant A,B --seeds 10

# De-MLS-like: one steward, bad links, members dropping offline
$R --n 50 --loss 5 --max-delay 10 --senders single --bad-frac 0,0.2 --offline-frac 0,0.3 --variant A,B --seeds 10

# Bandwidth with real commit sizes
$R --n 50 --loss 10 --max-delay 10 --payload 0,20000 --variant A,B --seeds 5

# Post-finality traffic
$R --n 30 --loss 10 --max-delay 10 --sync 30,120 --run-to-end --seeds 5
```

## Results so far

All runs use the default `--votes/--admit received`, N = 50, and uniform delay
unless noted. Each line comes from 5–10 seeds.

- **Safety.** No conflicting order in any run.
- **Completion.** Every node finalized everything, up to 40% loss.
- **Latency with B** is about the maximum delay plus a few seconds, and does not
  depend on loss or N:
  - 10 s max delay: 10–13 s, at both N = 50 and N = 100;
  - 15 s max delay: 17–22 s at 20–40% loss;
  - 30 s max delay: 25–28 s.
- **Latency with A** degrades with loss. At 15 s max delay it is 28 s at 20%
  loss, 126 s at 30% and 172 s at 40%.
- **SYNC dominates traffic.** At 10% loss with A, going from `--sync 30` to 120
  cuts packets from 2,680 to 720 with the same 13 s latency.
  - At 30% loss, A slows from 110 s to 190 s with sparse SYNC: A repairs through
    SDS-R, which needs SYNC as a carrier.
  - B is unaffected: 13 s either way.
- **Release order** was kept in every run, even with mains 5 s apart and 30 s
  delay. This is under i.i.d. delay; use `--delay geo` for a harsher test.
- **Bad links** (`--bad-frac 0.2 --bad-loss 60`): A can hit a permanent split
  vote, which is the spec's open question on split votes. B finished in every run
  tried.
- **B's cost** is about 2× the bytes of A with 20 kB payloads, because
  re-gossip re-sends the payload.
- **Traffic continues after finality.** In one run at N = 30 with default
  SYNC, SDS repair and SYNC sent 5,200 more packets during the 15 min after
  everyone finalized (`--run-to-end`).
- **Waiting for SDS causal delivery** (`--votes/--admit delivered`) stalls
  finality under loss. One lost and evicted message blocks everything causally
  after it.

**Suggested configuration:** variant B with sparse SYNC (≥ 120 s, or `0`).

## nim-sds changes

All changes are in `patches/sim-hooks.patch`, and nim-sds's own tests pass
(67/67).

- **Virtual clock hook**, plus two simulation-only speedups behind flags:
  skipping persistence snapshots, and memoising repeated decodes.
- **Copy-free `incomingBuffer` loops** (always on). Upstream copies every
  buffered message on each receive; with the fix, output is identical and runs
  are 3.4× faster at N = 100.
