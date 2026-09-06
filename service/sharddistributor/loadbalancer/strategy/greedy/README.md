# Greedy swap fallback

Greedy prefers single-shard moves, but can get stuck when every individual
shard is too large to move beneficially. The swap fallback exchanges two shards
to escape some of those local optima.

For example, executor A owns loads `[8, 7]` and B owns `[6, 5]`. Their totals
are 15 and 11. No single move improves balance, but exchanging 7 and 5 gives
both executors a total load of 13.

## Behavior

- Keep planning beneficial single moves using the existing source ordering
  and destination selection.
- Only attempt a swap when no source has a beneficial single move to the
  selected destination, and at least two moves remain in the budget.
- Search one executor pair: the heaviest overloaded ACTIVE source with
  eligible shards, and the selected destination. The destination must be
  ACTIVE and below the lower hysteresis band. DRAINING executors can still
  shed individual shards, but cannot participate in swaps.
- Both shards must have measured smoothed load of at least `0.01`, have
  completed any configured cooldown, and not have moved earlier in the pass.
- Accept a swap only if it reduces the sum of squared executor loads and
  puts both executors inside the existing hysteresis bands.
- Charge two moves against the budget and finish the pass after the swap.
  There is at most one swap search per pass.

A swap produces two ordinary `plan.Move` entries. It uses the existing
assignment and handover machinery; the actual handovers need not finish
simultaneously. The planner uses the existing namespace snapshot and simulated
loads after any preceding moves.

## Search and implementation decisions

For source and destination executor loads `H` and `L`, the ideal net transfer
is `(H - L) / 2`. For a source shard with load `a`, the ideal destination shard
therefore has load `a - (H - L) / 2`.

Retain eligible source candidates collected for the single-move search.
Only when the fallback is needed, collect and sort the destination candidates.
For each source candidate, binary-search that ideal destination load and check
the neighbors on either side. Choose the best qualifying exchange.

This finds the best qualifying swap for the selected executor pair. It does
not search every executor pair or plan exchanges involving more than two shards.

We considered reservoir sampling with 32 candidates per executor. Binary
search avoids probabilistically missing useful swaps and removes the sample-size
choice. Its measured cost was small at the namespace sizes under consideration.

Single-move source lists remain unsorted. Maintaining sorted candidates across
successive moves would add bookkeeping for moved shards and assignment indices.
The expected workload has few single moves, so that additional machinery did
not look justified. Candidate collection does add allocations on busy passes.

The acceptance rule uses the existing bands instead of introducing a new
percentage-improvement setting. Requiring both executors to finish inside the
bands is a conservative way to justify two handovers. Smaller improvements are
deliberately skipped; this policy still needs staging evaluation.

## Cost

Let `N` be total shards, `E` executors, `B` the move budget, and `s` and `d`
the eligible candidates in the selected source and destination.

- Original planner worst case: `O(N + B * (N + E log E))`.
- Swap search adds one destination eligibility scan, `O(d log d)` sorting,
  and `O(s log d)` searching. It is not multiplied by the move budget.
- Candidate storage adds `O(s + d)` memory for the selected pair. Overall
  planner memory remains `O(N + E + B)`.

Local Apple M4 benchmarks of the full in-memory planner at 16k total shards
measured about 1.2 ms with 400 eligible shards and 1.5 ms with all shards eligible.
These are synthetic fallback fixtures, not worst-case bounds or production
latency predictions. They exclude etcd, snapshot decoding, and real logging.

## Validation and observability

Tests cover single-move priority, budget accounting, cooldowns, executor status,
updated assignment indices after a move, the band requirement, and the one-swap
limit. The binary search is compared with exhaustive pair enumeration.

The swap counter is `shard_distributor_shard_assign_load_based_swaps`.
The existing load-based move counter includes both moves in a swap.

Run the unit tests and local planner benchmarks with:

```sh
go test ./service/sharddistributor/loadbalancer/...
go test ./service/sharddistributor/loadbalancer/strategy/greedy -run '^$' -bench BenchmarkPlanRebalanceSwapFallback -benchmem
```
