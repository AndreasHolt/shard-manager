package greedy

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/cadence-workflow/shard-manager/common/log"
	"github.com/cadence-workflow/shard-manager/common/metrics"
	"github.com/cadence-workflow/shard-manager/common/types"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/store"
)

func TestPlanRebalanceSwapFallback(t *testing.T) {
	for _, test := range []struct {
		name             string
		loads            map[string][]float64
		budget           int
		cooldownExecutor string
		drainingExecutor string
		wantMoves        int
		wantSwap         bool
	}{
		{name: "escape single-move local optimum", budget: 2, wantMoves: 2, wantSwap: true},
		{name: "zero budget", budget: 0},
		{name: "one move cannot fund a swap", budget: 1},
		{name: "source cooldown", budget: 2, cooldownExecutor: "A"},
		{name: "destination cooldown", budget: 2, cooldownExecutor: "B"},
		{name: "cannot swap back onto draining source", budget: 2, drainingExecutor: "A"},
		{name: "cannot swap onto draining destination", budget: 2, drainingExecutor: "B"},
		{
			name: "ordinary move takes priority", budget: 3, wantMoves: 1,
			loads: map[string][]float64{"A": {8, 7, 1}, "B": {6, 5}},
		},
		{
			name: "swap uses loads and indices after ordinary move", budget: 3, wantMoves: 3, wantSwap: true,
			loads: map[string][]float64{"A": {1, 8, 7}, "B": {6, 4}},
		},
		{
			name: "ordinary move leaves insufficient budget", budget: 2, wantMoves: 1,
			loads: map[string][]float64{"A": {1, 8, 7}, "B": {6, 4}},
		},
		{
			name: "one swap despite remaining imbalance and budget", budget: 8, wantMoves: 2, wantSwap: true,
			loads: map[string][]float64{"A": {8, 7}, "B": {6, 5}, "C": {8, 7}, "D": {6, 5}},
		},
		{
			name: "already inside bands", budget: 2,
			loads: map[string][]float64{"A": {8, 5}, "B": {7, 6}},
		},
		{
			name: "positive benefit does not resolve imbalance", budget: 2,
			loads: map[string][]float64{"A": {7.6, 7.4}, "B": {7.3, 3.7}},
		},
		{
			name: "exchanging whole loads has no benefit", budget: 2,
			loads: map[string][]float64{"A": {8}, "B": {6}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
			if test.loads == nil {
				test.loads = map[string][]float64{"A": {8, 7}, "B": {6, 5}}
			}
			assignments, state := swapTestState(test.loads, now)
			for _, shardID := range assignments[test.cooldownExecutor] {
				stats := state.ShardStats[shardID]
				stats.LastMoveTime = now
				state.ShardStats[shardID] = stats
			}
			if test.drainingExecutor != "" {
				state.Executors[test.drainingExecutor] = store.HeartbeatState{Status: types.ExecutorStatusDRAINING}
			}
			cfg := testGreedyConfig()
			cfg.MoveBudgetProportion = func(string) float64 { return float64(test.budget) / float64(len(state.ShardStats)) }
			testScope := tally.NewTestScope("test", nil)
			metricsScope := metrics.NewClient(testScope, metrics.ShardDistributor, metrics.MigrationConfig{}).Scope(metrics.ShardDistributorAssignLoopScope)
			originalAssignments := cloneAssignments(assignments)

			moves, err := PlanRebalance(cfg, testNamespace, state, assignments, now, log.NewNoop(), metricsScope)
			require.NoError(t, err)
			require.Len(t, moves, test.wantMoves)
			assert.Equal(t, originalAssignments, assignments, "planning must not mutate its input")
			movedShards := make(map[string]struct{})
			for _, move := range moves {
				assert.NotContains(t, movedShards, move.ShardID)
				movedShards[move.ShardID] = struct{}{}
			}
			applyMoves(t, assignments, moves)
			finalLoads, mean, _ := computeExecutorLoads(assignments, state)
			if test.wantSwap {
				swap := moves[len(moves)-2:]
				assert.Equal(t, swap[0].From, swap[1].To)
				assert.Equal(t, swap[0].To, swap[1].From)
				for _, move := range swap {
					assert.InDelta(t, mean, finalLoads[move.To], 1e-9)
				}
			}
			counts := make(map[string]int64)
			for _, counter := range testScope.Snapshot().Counters() {
				counts[counter.Name()] = counter.Value()
			}
			assert.Equal(t, int64(test.wantMoves), counts["test.shard_distributor_shard_assign_load_based_moves"])
			wantSwaps := int64(0)
			if test.wantSwap {
				wantSwaps = 1
			}
			assert.Equal(t, wantSwaps, counts["test.shard_distributor_shard_assign_load_based_swaps"])
		})
	}
}

func swapTestState(loads map[string][]float64, now time.Time) (map[string][]string, *store.NamespaceState) {
	assignments := make(map[string][]string)
	state := &store.NamespaceState{
		Executors:        make(map[string]store.HeartbeatState),
		ShardAssignments: make(map[string]store.AssignedState),
		ShardStats:       make(map[string]store.ShardStatistics),
	}
	for executorID, shardLoads := range loads {
		state.Executors[executorID] = store.HeartbeatState{Status: types.ExecutorStatusACTIVE, LastHeartbeat: now}
		assigned := store.AssignedState{AssignedShards: make(map[string]*types.ShardAssignment)}
		for i, load := range shardLoads {
			id := fmt.Sprintf("%s-%d", executorID, i)
			assignments[executorID] = append(assignments[executorID], id)
			assigned.AssignedShards[id] = &types.ShardAssignment{Status: types.AssignmentStatusREADY}
			state.ShardStats[id] = store.ShardStatistics{SmoothedLoad: load, LastUpdateTime: now}
		}
		state.ShardAssignments[executorID] = assigned
	}
	return assignments, state
}

func TestFindBestSwapMatchesExhaustiveSearch(t *testing.T) {
	for _, test := range []struct {
		name                          string
		sourceCount, destinationCount int
	}{
		{name: "empty source", destinationCount: 5},
		{name: "empty destination", sourceCount: 5},
		{name: "single candidate", sourceCount: 1, destinationCount: 1},
		{name: "unequal candidate counts", sourceCount: 20, destinationCount: 3},
		{name: "400 eligible shards", sourceCount: 200, destinationCount: 200},
	} {
		t.Run(test.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(42))
			for attempt := 0; attempt < 30; attempt++ {
				shards := func(count int) []eligibleShard {
					result := make([]eligibleShard, count)
					for i := range result {
						// Discrete loads exercise duplicate loads and exact target matches.
						result[i] = eligibleShard{shardID: fmt.Sprint(i), assignmentIndex: i, load: float64(1+rng.Intn(100)) / 10}
					}
					return result
				}
				source, destination := shards(test.sourceCount), shards(test.destinationCount)
				high, low := 10+rng.Float64()*10, rng.Float64()*10
				mean := (high+low)/2 + rng.Float64()*4 - 2
				lower, upper := mean*.9, mean*1.15
				bestBenefit := 0.0
				for _, a := range source {
					for _, b := range destination {
						delta := a.load - b.load
						x, y := high-delta, low+delta
						if delta > 0 && x >= lower && x <= upper && y >= lower && y <= upper {
							bestBenefit = max(bestBenefit, 2*delta*(high-low-delta))
						}
					}
				}
				pair, found := findBestSwap(source, destination, high, low, lower, upper)
				require.Equal(t, bestBenefit > 0, found, "attempt %d", attempt)
				if found {
					assert.Contains(t, source, pair[0])
					assert.Contains(t, destination, pair[1])
					x, y := high-pair[0].load+pair[1].load, low+pair[0].load-pair[1].load
					assert.InDelta(t, bestBenefit, high*high+low*low-x*x-y*y, 1e-9)
				}
			}
		})
	}
}

func BenchmarkPlanRebalanceSwapFallback(b *testing.B) {
	destination400 := make([]float64, 398)
	for i := range destination400 {
		destination400[i] = .01
	}
	destination400[0], destination400[1] = 5, 2.04
	destination16000 := make([]float64, 15998)
	for i := range destination16000 {
		destination16000[i] = .01
	}
	destination16000[0] = 90
	for _, scenario := range []struct {
		name  string
		loads map[string][]float64
	}{
		{name: "balanced", loads: map[string][]float64{"A": {8, 5}, "B": {7, 6}}},
		{name: "stalled", loads: map[string][]float64{"A": {7.6, 7.4}, "B": {7.3, 3.7}}},
		{name: "swappable", loads: map[string][]float64{"A": {8, 7}, "B": {6, 5}}},
		{name: "400_eligible", loads: map[string][]float64{"A": {8, 7}, "B": destination400}},
		{name: "16000_eligible", loads: map[string][]float64{"A": {170, 190}, "B": destination16000}},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			loads := make(map[string][]float64)
			padding := (16000 - len(scenario.loads["A"]) - len(scenario.loads["B"])) / 2
			for _, executorID := range []string{"A", "B"} {
				loads[executorID] = make([]float64, len(scenario.loads[executorID])+padding)
				copy(loads[executorID], scenario.loads[executorID])
			}
			now := time.Now()
			assignments, state := swapTestState(loads, now)
			cfg := testGreedyConfig()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := PlanRebalance(cfg, testNamespace, state, assignments, now, log.NewNoop(), metrics.NoopScope)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
