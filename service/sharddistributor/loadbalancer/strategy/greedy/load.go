package greedy

import "github.com/cadence-workflow/shard-manager/service/sharddistributor/store"

// averageMeasuredShardLoad calculates the average smoothed load of all
// shards in the assignments that have received at least one load report.
func averageMeasuredShardLoad(
	assignments map[string][]string,
	shardStats map[string]store.ShardStatistics,
) float64 {
	var sum float64
	var count int
	for _, shards := range assignments {
		for _, shardID := range shards {
			stats, ok := shardStats[shardID]
			if !ok || !hasSmoothedLoadUpdate(stats) {
				continue
			}
			sum += stats.SmoothedLoad
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}

// effectiveShardLoad returns the shard's smoothed load if it has received at
// least one load report, otherwise the average measured shard load.
func effectiveShardLoad(
	shardID string,
	shardStats map[string]store.ShardStatistics,
	averageMeasured float64,
) float64 {
	stats, ok := shardStats[shardID]
	if !ok || !hasSmoothedLoadUpdate(stats) {
		return averageMeasured
	}
	return stats.SmoothedLoad
}

// hasSmoothedLoadUpdate reports whether the shard has received at least one load report.
func hasSmoothedLoadUpdate(stats store.ShardStatistics) bool {
	return !stats.LastUpdateTime.IsZero()
}
