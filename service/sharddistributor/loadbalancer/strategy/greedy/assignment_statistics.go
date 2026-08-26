package greedy

import (
	"time"

	"github.com/cadence-workflow/shard-manager/service/sharddistributor/store"
)

// PrepareAssignmentStatistics returns complete statistics maps for executors
// affected by an assignment change.
func PrepareAssignmentStatistics(
	previousAssignments map[string]store.AssignedState,
	newAssignments map[string]store.AssignedState,
	previousStatistics map[string]store.ShardStatistics,
	now time.Time,
) []store.ExecutorShardStatistics {
	previousOwners := store.ShardOwners(previousAssignments)
	affectedExecutors := findAffectedExecutors(previousOwners, newAssignments)
	executorStatisticsUpdates := buildStatisticsUpdates(
		affectedExecutors,
		previousOwners,
		newAssignments,
		previousStatistics,
		now,
	)
	return executorStatisticsUpdates
}

func findAffectedExecutors(
	previousOwners map[string]string,
	newAssignments map[string]store.AssignedState,
) map[string]struct{} {
	affectedExecutors := make(map[string]struct{})
	for newOwnerID, assignedState := range newAssignments {
		for shardID := range assignedState.AssignedShards {
			previousOwnerID, previouslyAssigned := previousOwners[shardID]
			if previouslyAssigned && previousOwnerID == newOwnerID {
				continue
			}

			affectedExecutors[newOwnerID] = struct{}{}
			if previouslyAssigned {
				affectedExecutors[previousOwnerID] = struct{}{}
			}
		}
	}
	return affectedExecutors
}

func buildStatisticsUpdates(
	affectedExecutors map[string]struct{},
	previousOwners map[string]string,
	newAssignments map[string]store.AssignedState,
	previousStatistics map[string]store.ShardStatistics,
	now time.Time,
) []store.ExecutorShardStatistics {
	updates := make([]store.ExecutorShardStatistics, 0, len(affectedExecutors))
	for executorID := range affectedExecutors {
		assignedState := newAssignments[executorID]
		statistics := make(map[string]store.ShardStatistics, len(assignedState.AssignedShards))
		for shardID := range assignedState.AssignedShards {
			previousOwnerID, previouslyAssigned := previousOwners[shardID]
			shardStatistics, hasPreviousStatistics := previousStatistics[shardID]
			sameOwner := previouslyAssigned && previousOwnerID == executorID

			if sameOwner {
				if hasPreviousStatistics {
					statistics[shardID] = shardStatistics
				}
			} else {
				// A moved shard carries its existing load history and starts a new move cooldown.
				// A newly assigned or previously unmeasured shard starts with zero statistics.
				if previouslyAssigned && hasPreviousStatistics {
					shardStatistics.LastMoveTime = now
				} else {
					shardStatistics = store.ShardStatistics{}
				}
				statistics[shardID] = shardStatistics
			}
		}
		updates = append(updates, store.ExecutorShardStatistics{
			ExecutorID: executorID,
			Statistics: statistics,
		})
	}

	return updates
}
