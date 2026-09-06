package greedy

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/cadence-workflow/shard-manager/common/log"
	"github.com/cadence-workflow/shard-manager/common/log/tag"
	"github.com/cadence-workflow/shard-manager/common/metrics"
	"github.com/cadence-workflow/shard-manager/common/types"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/config"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/loadbalancer/plan"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/store"
)

const minShardSmoothedLoadForMove = 0.01

type moveCandidate struct {
	shardID         string
	from            string
	to              string
	assignmentIndex int
}

// PlanRebalance returns planned shard moves for the current assignment state.
func PlanRebalance(
	cfg config.LoadBalancingGreedyConfig,
	namespace string,
	namespaceState *store.NamespaceState,
	currentAssignments map[string][]string,
	now time.Time,
	logger log.Logger,
	metricsScope metrics.Scope,
) ([]plan.Move, error) {
	now = now.UTC()
	workingAssignments := cloneAssignments(currentAssignments)
	loads, meanLoad, ok := computeExecutorLoads(workingAssignments, namespaceState)
	if !ok {
		return nil, nil
	}

	totalShards := 0
	for _, shards := range currentAssignments {
		totalShards += len(shards)
	}
	moveBudget := computeMoveBudget(totalShards, cfg.MoveBudgetProportion(namespace))
	if moveBudget <= 0 {
		return nil, nil
	}
	moves := make([]plan.Move, 0, moveBudget)
	movedShards := make(map[string]struct{})

	// Plan multiple moves per cycle (within budget), recomputing eligibility after each move.
	// Stop early once sources/destinations are empty, i.e. imbalance is within hysteresis bands.
	for moveBudget > 0 {
		candidates := findNextMoves(cfg, namespace, namespaceState, workingAssignments, loads, meanLoad, movedShards, now, moveBudget)
		if len(candidates) == 0 {
			break
		}
		for _, candidate := range candidates {
			if err := applyMoveCandidate(workingAssignments, candidate); err != nil {
				return nil, err
			}
			movedShards[candidate.shardID] = struct{}{}
			updateExecutorLoadsAfterMove(namespaceState, candidate.from, candidate.to, loads, candidate.shardID)
			move := plan.Move{ShardID: candidate.shardID, From: candidate.from, To: candidate.to}
			moves = append(moves, move)
			shardLoad := namespaceState.ShardStats[move.ShardID].SmoothedLoad
			logGreedyMove(logger, loads, move, shardLoad)
			if metricsScope != nil {
				metricsScope.UpdateGauge(metrics.ShardDistributorAssignLoopMovedShardLoad, shardLoad)
			}
			moveBudget--
		}
		// A swap ends the pass so its search cost is paid at most once.
		if len(candidates) == 2 {
			if metricsScope != nil {
				metricsScope.AddCounter(metrics.ShardDistributorAssignLoopLoadBasedSwaps, 1)
			}
			break
		}
	}
	if len(moves) > 0 && metricsScope != nil {
		metricsScope.AddCounter(metrics.ShardDistributorAssignLoopLoadBasedMoves, int64(len(moves)))
	}
	return moves, nil
}

func cloneAssignments(assignments map[string][]string) map[string][]string {
	cloned := make(map[string][]string, len(assignments))
	for executorID, shardIDs := range assignments {
		clonedShards := make([]string, len(shardIDs))
		copy(clonedShards, shardIDs)
		cloned[executorID] = clonedShards
	}
	return cloned
}

func computeExecutorLoads(currentAssignments map[string][]string, state *store.NamespaceState) (map[string]float64, float64, bool) {
	if len(currentAssignments) == 0 {
		return nil, 0, false
	}

	averageMeasured := averageMeasuredShardLoad(currentAssignments, state.ShardStats)
	loads := make(map[string]float64, len(currentAssignments))
	total := 0.0
	for executorID, shards := range currentAssignments {
		loads[executorID] = 0
		for _, shardID := range shards {
			load := effectiveShardLoad(shardID, state.ShardStats, averageMeasured)
			loads[executorID] += load
			total += load
		}
	}

	mean := total / float64(len(currentAssignments))
	return loads, mean, true
}

func computeMoveBudget(totalShards int, proportion float64) int {
	if totalShards <= 0 || proportion <= 0 {
		return 0
	}
	return int(math.Ceil(proportion * float64(totalShards)))
}

// findNextMoves prefers a single move, falling back to one swap between active
// executors only when no source has a beneficial single move.
func findNextMoves(
	cfg config.LoadBalancingGreedyConfig,
	namespace string,
	namespaceState *store.NamespaceState,
	workingAssignments map[string][]string,
	loads map[string]float64,
	meanLoad float64,
	movedShards map[string]struct{},
	now time.Time,
	moveBudget int,
) []moveCandidate {
	upperBand, lowerBand := cfg.HysteresisUpperBand(namespace), cfg.HysteresisLowerBand(namespace)
	sources, destinations := classifySourcesAndDestinations(loads, namespaceState, meanLoad, upperBand, lowerBand)
	if len(sources) == 0 {
		return nil
	}
	destination, ok := selectDestinationExecutor(destinations, workingAssignments, namespaceState, loads, meanLoad, cfg.SevereImbalanceRatio(namespace))
	if !ok {
		return nil
	}

	cooldown := cfg.PerShardCooldown(namespace)
	var swapShards []eligibleShard
	swapSource := ""
	sortByDescendingLoad(sources, loads)
	for _, source := range sources {
		if source == destination {
			continue
		}
		shards := collectEligibleShards(workingAssignments[source], namespaceState.ShardStats, movedShards, now, cooldown)
		if shard, found := findBestShardForMove(shards, loads[source], loads[destination]); found {
			return []moveCandidate{shard.move(source, destination)}
		}
		// Retain the heaviest active source with eligible shards for one swap attempt.
		if len(swapShards) == 0 && namespaceState.Executors[source].Status == types.ExecutorStatusACTIVE {
			swapSource, swapShards = source, shards
		}
	}

	if moveBudget < 2 || len(swapShards) == 0 || loads[destination] >= meanLoad*lowerBand {
		return nil
	}
	destinationShards := collectEligibleShards(workingAssignments[destination], namespaceState.ShardStats, movedShards, now, cooldown)
	pair, found := findBestSwap(swapShards, destinationShards, loads[swapSource], loads[destination], meanLoad*lowerBand, meanLoad*upperBand)
	if !found {
		return nil
	}
	return []moveCandidate{pair[0].move(swapSource, destination), pair[1].move(destination, swapSource)}
}

// selectDestinationExecutor picks the least-loaded destination executor. If
// there are no destination executors, it falls back to all ACTIVE executors only
// when the namespace is severely imbalanced.
func selectDestinationExecutor(
	destinationExecutors []string,
	workingAssignments map[string][]string,
	namespaceState *store.NamespaceState,
	loads map[string]float64,
	meanLoad float64,
	severeImbalanceRatio float64,
) (string, bool) {
	if len(destinationExecutors) == 0 {
		if !isSevereImbalance(loads, meanLoad, severeImbalanceRatio) {
			return "", false
		}
		allActiveExecutors := make([]string, 0, len(workingAssignments))
		for executorID := range workingAssignments {
			if namespaceState.Executors[executorID].Status == types.ExecutorStatusACTIVE {
				allActiveExecutors = append(allActiveExecutors, executorID)
			}
		}
		if len(allActiveExecutors) == 0 {
			return "", false
		}
		destinationExecutors = allActiveExecutors
	}

	return findBestDestination(destinationExecutors, loads)
}

func classifySourcesAndDestinations(
	executorLoads map[string]float64,
	state *store.NamespaceState,
	meanLoad float64,
	upperBand float64,
	lowerBand float64,
) ([]string, []string) {
	sources := make([]string, 0)
	destinations := make([]string, 0)

	for executorID, load := range executorLoads {
		executor := state.Executors[executorID]
		// Intentionally allow DRAINING executors as sources so they can shed shards
		if load > meanLoad*upperBand {
			sources = append(sources, executorID)
		} else if executor.Status == types.ExecutorStatusACTIVE && load < meanLoad*lowerBand {
			destinations = append(destinations, executorID)
		}
	}

	return sources, destinations
}

func isSevereImbalance(executorLoads map[string]float64, meanLoad, severeImbalanceRatio float64) bool {
	if meanLoad <= 0 || severeImbalanceRatio <= 0 {
		return false
	}

	maxLoad := 0.0
	for _, load := range executorLoads {
		if load > maxLoad {
			maxLoad = load
		}
	}
	return maxLoad/meanLoad >= severeImbalanceRatio
}

func findBestDestination(destinationExecutors []string, executorLoads map[string]float64) (string, bool) {
	minExecutor := ""
	found := false
	var minLoad float64
	for _, executor := range destinationExecutors {
		load := executorLoads[executor]
		if !found || load < minLoad {
			minLoad = load
			minExecutor = executor
			found = true
		}
	}
	return minExecutor, found
}

func sortByDescendingLoad(executors []string, executorLoads map[string]float64) {
	slices.SortFunc(executors, func(a, b string) int {
		return cmp.Compare(executorLoads[b], executorLoads[a])
	})
}

type eligibleShard struct {
	shardID         string
	assignmentIndex int
	load            float64
}

func (s eligibleShard) move(from, to string) moveCandidate {
	return moveCandidate{shardID: s.shardID, assignmentIndex: s.assignmentIndex, from: from, to: to}
}

func collectEligibleShards(
	shardIDs []string,
	statistics map[string]store.ShardStatistics,
	movedShards map[string]struct{},
	now time.Time,
	perShardCooldown time.Duration,
) []eligibleShard {
	var shards []eligibleShard
	for i, shardID := range shardIDs {
		if _, moved := movedShards[shardID]; moved {
			continue
		}
		stats, ok := statistics[shardID]
		if !ok || !hasSmoothedLoadUpdate(stats) {
			continue
		}
		if stats.SmoothedLoad < minShardSmoothedLoadForMove {
			continue
		}
		if perShardCooldown > 0 && !stats.LastMoveTime.IsZero() && now.Sub(stats.LastMoveTime) < perShardCooldown {
			continue
		}
		shards = append(shards, eligibleShard{shardID: shardID, assignmentIndex: i, load: stats.SmoothedLoad})
	}
	return shards
}

func findBestShardForMove(shards []eligibleShard, sourceLoad, destinationLoad float64) (eligibleShard, bool) {
	var best eligibleShard
	bestBenefit := 0.0
	for _, shard := range shards {
		benefit := computeBenefitOfMove(sourceLoad, destinationLoad, shard.load)
		if benefit > bestBenefit {
			bestBenefit, best = benefit, shard
		}
	}
	return best, bestBenefit > 0
}

// computeBenefitOfMove returns the reduction in squared executor load from
// moving shardLoad from source to destination. Positive values mean the move
// improves balance between the two executors.
func computeBenefitOfMove(sourceLoad, destLoad, shardLoad float64) float64 {
	squaredLoadBeforeMove := sourceLoad*sourceLoad + destLoad*destLoad
	afterSourceLoad := sourceLoad - shardLoad
	afterDestLoad := destLoad + shardLoad
	squaredLoadAfterMove := afterSourceLoad*afterSourceLoad + afterDestLoad*afterDestLoad
	return squaredLoadBeforeMove - squaredLoadAfterMove
}

// applyMoveCandidate applies a planned move to the in-memory assignment state.
func applyMoveCandidate(currentAssignments map[string][]string, candidate moveCandidate) error {
	if candidate.assignmentIndex < 0 || candidate.assignmentIndex >= len(currentAssignments[candidate.from]) {
		return fmt.Errorf("candidate assignment index out of range for shard %s on source executor %s", candidate.shardID, candidate.from)
	}
	if currentAssignments[candidate.from][candidate.assignmentIndex] != candidate.shardID {
		return fmt.Errorf("candidate assignment index mismatch for shard %s on source executor %s", candidate.shardID, candidate.from)
	}

	currentAssignments[candidate.from][candidate.assignmentIndex] = currentAssignments[candidate.from][len(currentAssignments[candidate.from])-1]
	currentAssignments[candidate.from] = currentAssignments[candidate.from][:len(currentAssignments[candidate.from])-1]
	currentAssignments[candidate.to] = append(currentAssignments[candidate.to], candidate.shardID)
	return nil
}

func updateExecutorLoadsAfterMove(
	state *store.NamespaceState,
	source string,
	destination string,
	executorLoads map[string]float64,
	shardID string,
) {
	stats, ok := state.ShardStats[shardID]
	if !ok {
		return
	}
	executorLoads[source] -= stats.SmoothedLoad
	executorLoads[destination] += stats.SmoothedLoad
}

func logGreedyMove(logger log.Logger, loads map[string]float64, move plan.Move, shardLoad float64) {
	sourceLoadBefore := loads[move.From] + shardLoad
	destinationLoadBefore := loads[move.To] - shardLoad
	logger.Info("Greedy load-based shard move",
		tag.ShardKey(move.ShardID),
		tag.ShardExecutor(move.From),
		tag.Dynamic("destination_executor", move.To),
		tag.ShardLoad(fmt.Sprintf("%f", shardLoad)),
		tag.Dynamic("source_executor_load_before", sourceLoadBefore),
		tag.Dynamic("destination_executor_load_before", destinationLoadBefore),
	)
}
