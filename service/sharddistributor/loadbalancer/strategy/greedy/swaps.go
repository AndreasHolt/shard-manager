package greedy

import (
	"cmp"
	"slices"
)

// findBestSwap sorts only destinationShards and leaves sourceShards in assignment order.
func findBestSwap(sourceShards, destinationShards []eligibleShard, sourceLoad, destinationLoad, lowerBound, upperBound float64) ([2]eligibleShard, bool) {
	slices.SortFunc(destinationShards, func(a, b eligibleShard) int {
		return cmp.Compare(a.load, b.load)
	})
	idealTransfer := (sourceLoad - destinationLoad) / 2
	var best [2]eligibleShard
	bestBenefit := 0.0
	for _, source := range sourceShards {
		// Squared-load benefit is maximized when the pair's final loads are equal.
		// The two neighbors of this target cover the best discrete exchange.
		index, _ := slices.BinarySearchFunc(destinationShards, source.load-idealTransfer, func(shard eligibleShard, target float64) int {
			return cmp.Compare(shard.load, target)
		})
		for i := max(0, index-1); i < min(len(destinationShards), index+1); i++ {
			destination := destinationShards[i]
			transfer := source.load - destination.load
			if transfer <= 0 {
				continue
			}
			// Two handovers must bring both executors inside the existing bands.
			afterSource, afterDestination := sourceLoad-transfer, destinationLoad+transfer
			if afterSource < lowerBound || afterSource > upperBound || afterDestination < lowerBound || afterDestination > upperBound {
				continue
			}
			benefit := computeBenefitOfMove(sourceLoad, destinationLoad, transfer)
			if benefit > bestBenefit {
				bestBenefit, best = benefit, [2]eligibleShard{source, destination}
			}
		}
	}
	return best, bestBenefit > 0
}
