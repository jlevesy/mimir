// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/sharding_strategy.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extprom"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

const (
	shardExcludedMeta = "shard-excluded"
)

type ShardingStrategy interface {
	// FilterUsers whose blocks should be loaded by the store-gateway. Returns the list of user IDs
	// that should be synced by the store-gateway.
	FilterUsers(ctx context.Context, userIDs []string) []string

	// FilterBlocks filters metas in-place keeping only blocks that should be loaded by the store-gateway.
	// The provided loaded map contains blocks which have been previously returned by this function and
	// are now loaded or loading in the store-gateway.
	FilterBlocks(ctx context.Context, userID string, metas map[ulid.ULID]*metadata.Meta, loaded map[ulid.ULID]struct{}, synced *extprom.TxGaugeVec) error
}

// ShardingLimits is the interface that should be implemented by the limits provider,
// limiting the scope of the limits to the ones required by sharding strategies.
type ShardingLimits interface {
	StoreGatewayTenantShardSize(userID string) int
}

// ShuffleShardingStrategy is a shuffle sharding strategy, based on the hash ring formed by store-gateways,
// where each tenant blocks are sharded across a subset of store-gateway instances.
type ShuffleShardingStrategy struct {
	r            *ring.Ring
	instanceID   string
	instanceAddr string
	limits       ShardingLimits
	logger       log.Logger
}

// NewShuffleShardingStrategy makes a new ShuffleShardingStrategy.
func NewShuffleShardingStrategy(r *ring.Ring, instanceID, instanceAddr string, limits ShardingLimits, logger log.Logger) *ShuffleShardingStrategy {
	return &ShuffleShardingStrategy{
		r:            r,
		instanceID:   instanceID,
		instanceAddr: instanceAddr,
		limits:       limits,
		logger:       logger,
	}
}

// FilterUsers implements ShardingStrategy.
func (s *ShuffleShardingStrategy) FilterUsers(_ context.Context, userIDs []string) []string {
	var filteredIDs []string

	for _, userID := range userIDs {
		subRing := GetShuffleShardingSubring(s.r, userID, s.limits)

		// Include the user only if it belongs to this store-gateway shard.
		if subRing.HasInstance(s.instanceID) {
			filteredIDs = append(filteredIDs, userID)
		}
	}

	return filteredIDs
}

// FilterBlocks implements ShardingStrategy.
func (s *ShuffleShardingStrategy) FilterBlocks(_ context.Context, userID string, metas map[ulid.ULID]*metadata.Meta, loaded map[ulid.ULID]struct{}, synced *extprom.TxGaugeVec) error {
	// As a protection, ensure the store-gateway instance is healthy in the ring. If it's unhealthy because it's failing
	// to heartbeat or get updates from the ring, or even removed from the ring because of the auto-forget feature, then
	// keep the previously loaded blocks.
	// TODO test
	if set, err := s.r.GetAllHealthy(BlocksOwnerSync); err != nil || !set.Includes(s.instanceAddr) {
		for blockID := range metas {
			if _, ok := loaded[blockID]; ok {
				level.Warn(s.logger).Log("msg", "store-gateway is unhealthy in the ring but block is kept because was previously loaded", "block", blockID.String(), "err", err)
			} else {
				level.Warn(s.logger).Log("msg", "store-gateway is unhealthy in the ring and block has been excluded because was not previously loaded", "block", blockID.String(), "err", err)

				// Skip the block.
				synced.WithLabelValues(shardExcludedMeta).Inc()
				delete(metas, blockID)
			}
		}

		return nil
	}

	r := GetShuffleShardingSubring(s.r, userID, s.limits)
	bufDescs, bufHosts, bufZones := ring.MakeBuffersForGet()

	for blockID := range metas {
		key := mimir_tsdb.HashBlockID(blockID)

		// Check if the block is owned by the store-gateway
		set, err := r.Get(key, BlocksOwnerSync, bufDescs, bufHosts, bufZones)

		// If an error occurs while checking the ring, we keep the previously loaded blocks.
		if err != nil {
			if _, ok := loaded[blockID]; ok {
				level.Warn(s.logger).Log("msg", "failed to check block owner but block is kept because was previously loaded", "block", blockID.String(), "err", err)
			} else {
				level.Warn(s.logger).Log("msg", "failed to check block owner and block has been excluded because was not previously loaded", "block", blockID.String(), "err", err)

				// Skip the block.
				synced.WithLabelValues(shardExcludedMeta).Inc()
				delete(metas, blockID)
			}

			continue
		}

		// Keep the block if it is owned by the store-gateway.
		if set.Includes(s.instanceAddr) {
			continue
		}

		// The block is not owned by the store-gateway. However, if it's currently loaded
		// we can safely unload it only once at least 1 authoritative owner is available
		// for queries.
		if _, ok := loaded[blockID]; ok {
			// The ring Get() returns an error if there's no available instance.
			if _, err := r.Get(key, BlocksOwnerRead, bufDescs, bufHosts, bufZones); err != nil {
				// Keep the block.
				continue
			}
		}

		// The block is not owned by the store-gateway and there's at least 1 available
		// authoritative owner available for queries, so we can filter it out (and unload
		// it if it was loaded).
		synced.WithLabelValues(shardExcludedMeta).Inc()
		delete(metas, blockID)
	}

	return nil
}

// GetShuffleShardingSubring returns the subring to be used for a given user. This function
// should be used both by store-gateway and querier in order to guarantee the same logic is used.
func GetShuffleShardingSubring(ring *ring.Ring, userID string, limits ShardingLimits) ring.ReadRing {
	shardSize := limits.StoreGatewayTenantShardSize(userID)

	// A shard size of 0 means shuffle sharding is disabled for this specific user,
	// so we just return the full ring so that blocks will be sharded across all store-gateways.
	if shardSize <= 0 {
		return ring
	}

	return ring.ShuffleShard(userID, shardSize)
}

type shardingMetadataFilterAdapter struct {
	userID   string
	strategy ShardingStrategy

	// Keep track of the last blocks returned by the Filter() function.
	lastBlocks map[ulid.ULID]struct{}
}

func NewShardingMetadataFilterAdapter(userID string, strategy ShardingStrategy) block.MetadataFilter {
	return &shardingMetadataFilterAdapter{
		userID:     userID,
		strategy:   strategy,
		lastBlocks: map[ulid.ULID]struct{}{},
	}
}

// Filter implements block.MetadataFilter.
// This function is NOT safe for use by multiple goroutines concurrently.
func (a *shardingMetadataFilterAdapter) Filter(ctx context.Context, metas map[ulid.ULID]*metadata.Meta, synced *extprom.TxGaugeVec, modified *extprom.TxGaugeVec) error {
	if err := a.strategy.FilterBlocks(ctx, a.userID, metas, a.lastBlocks, synced); err != nil {
		return err
	}

	// Keep track of the last filtered blocks.
	a.lastBlocks = make(map[ulid.ULID]struct{}, len(metas))
	for blockID := range metas {
		a.lastBlocks[blockID] = struct{}{}
	}

	return nil
}
