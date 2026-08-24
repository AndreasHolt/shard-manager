package greedy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cadence-workflow/shard-manager/common/log/testlogger"
	"github.com/cadence-workflow/shard-manager/common/types"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/store"
)

func TestPrepareShardStatistics(t *testing.T) {
	const (
		previousUpdatedShardLoad = 10.0
		preservedShardLoad       = 20.0
		updatedShardLoad         = 30.0
		unassignedShardLoad      = 1000.0
		expectedWarningCount     = 1
	)

	now := time.Now().UTC()
	previousUpdateTime := now.Add(-time.Minute)
	previousMoveTime := now.Add(-time.Hour)
	updatedShardID := "updated-shard"
	preservedShardID := "preserved-shard"
	unassignedShardID := "unassigned-shard"
	emptyReportShardID := "empty-report-shard"

	assignedState := &store.AssignedState{
		AssignedShards: map[string]*types.ShardAssignment{
			updatedShardID:     {Status: types.AssignmentStatusREADY},
			preservedShardID:   {Status: types.AssignmentStatusREADY},
			emptyReportShardID: {Status: types.AssignmentStatusREADY},
		},
	}
	previousStats := map[string]store.ShardStatistics{
		updatedShardID: {
			SmoothedLoad: previousUpdatedShardLoad,
			LastMoveTime: previousMoveTime,
		},
		preservedShardID: {
			SmoothedLoad:   preservedShardLoad,
			LastUpdateTime: previousUpdateTime,
			LastMoveTime:   previousMoveTime,
		},
	}
	reports := map[string]*types.ShardStatusReport{
		updatedShardID:     {ShardLoad: updatedShardLoad},
		unassignedShardID:  {ShardLoad: unassignedShardLoad},
		emptyReportShardID: nil,
	}
	logger, logs := testlogger.NewObserved(t)

	got, shouldWrite := PrepareShardStatistics(
		testGreedyConfig(),
		testNamespace,
		"executor-1",
		reports,
		assignedState,
		previousStats,
		now,
		logger,
	)

	require.True(t, shouldWrite)
	expectedStats := map[string]store.ShardStatistics{
		updatedShardID: {
			SmoothedLoad:   updatedShardLoad,
			LastUpdateTime: now,
			LastMoveTime:   previousMoveTime,
		},
		preservedShardID: previousStats[preservedShardID],
	}
	assert.Equal(t, expectedStats, got)
	assert.Equal(t, expectedWarningCount, logs.FilterMessage("empty report, skipping smoothed load update").Len())
}

func TestPrepareShardStatisticsReturnsNoUpdateWithoutEligibleReports(t *testing.T) {
	const unassignedShardLoad = 100.0

	assignedState := &store.AssignedState{
		AssignedShards: map[string]*types.ShardAssignment{
			"assigned-shard": {Status: types.AssignmentStatusREADY},
		},
	}
	reports := map[string]*types.ShardStatusReport{
		"unassigned-shard": {ShardLoad: unassignedShardLoad},
	}

	got, shouldWrite := PrepareShardStatistics(
		testGreedyConfig(),
		testNamespace,
		"executor-1",
		reports,
		assignedState,
		nil,
		time.Now().UTC(),
		testlogger.New(t),
	)

	assert.False(t, shouldWrite)
	assert.Empty(t, got)
}
