package greedy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cadence-workflow/shard-manager/common/types"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/store"
)

func TestPrepareAssignmentStatistics(t *testing.T) {
	now := time.Date(2026, time.August, 21, 0, 0, 0, 0, time.UTC)
	previousStatistics := map[string]store.ShardStatistics{
		"move": {
			SmoothedLoad:   12.5,
			LastUpdateTime: now.Add(-time.Minute),
			LastMoveTime:   now.Add(-time.Hour),
		},
		"stay-source": {
			SmoothedLoad:   3,
			LastUpdateTime: now.Add(-time.Minute),
		},
		"stay-destination": {
			SmoothedLoad:   7,
			LastUpdateTime: now.Add(-time.Minute),
		},
	}
	previousAssignments := map[string]store.AssignedState{
		"source": {
			AssignedShards: map[string]*types.ShardAssignment{
				"move":        {Status: types.AssignmentStatusREADY},
				"stay-source": {Status: types.AssignmentStatusREADY},
			},
		},
		"destination": {
			AssignedShards: map[string]*types.ShardAssignment{
				"stay-destination": {Status: types.AssignmentStatusREADY},
			},
		},
	}
	newAssignments := map[string]store.AssignedState{
		"source": {
			AssignedShards: map[string]*types.ShardAssignment{
				"stay-source": {Status: types.AssignmentStatusREADY},
			},
		},
		"destination": {
			AssignedShards: map[string]*types.ShardAssignment{
				"move":             {Status: types.AssignmentStatusREADY},
				"stay-destination": {Status: types.AssignmentStatusREADY},
				"new":              {Status: types.AssignmentStatusREADY},
			},
		},
	}

	updates := PrepareAssignmentStatistics(
		previousAssignments,
		newAssignments,
		previousStatistics,
		now,
	)

	updatesByExecutor := make(map[string]map[string]store.ShardStatistics, len(updates))
	for _, update := range updates {
		updatesByExecutor[update.ExecutorID] = update.Statistics
	}

	require.Len(t, updatesByExecutor, 2)
	assert.Equal(t, map[string]store.ShardStatistics{
		"stay-source": previousStatistics["stay-source"],
	}, updatesByExecutor["source"])

	destinationStatistics := updatesByExecutor["destination"]
	require.Len(t, destinationStatistics, 3)
	assert.Equal(t, previousStatistics["stay-destination"], destinationStatistics["stay-destination"])
	assert.Equal(t, store.ShardStatistics{}, destinationStatistics["new"])

	movedStatistics := destinationStatistics["move"]
	assert.Equal(t, previousStatistics["move"].SmoothedLoad, movedStatistics.SmoothedLoad)
	assert.Equal(t, previousStatistics["move"].LastUpdateTime, movedStatistics.LastUpdateTime)
	assert.Equal(t, now, movedStatistics.LastMoveTime)
}
