package shardcache

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/cadence-workflow/shard-manager/common/clock"
	"github.com/cadence-workflow/shard-manager/common/log"
	"github.com/cadence-workflow/shard-manager/common/log/tag"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/store"
)

type executorStateSubscriber struct {
	updates               chan map[*store.ShardOwner][]string
	pendingUpdateSince    time.Time
	hasPendingUpdateSince bool
}

// executorStatePubSub manages subscriptions to executor state changes.
//
// Each subscriber has a buffered (size 1) channel. When a subscriber can't
// keep up, publish drains the stale pending message and replaces it with
// the latest state, so the subscriber always catches up to the most recent
// state rather than being stuck on a stale intermediate state.
type executorStatePubSub struct {
	mu                 sync.Mutex
	subscribers        map[string]*executorStateSubscriber
	logger             log.Logger
	namespace          string
	timeSource         clock.TimeSource
	lastPublishedAt    time.Time
	hasPreviousPublish bool
}

func newExecutorStatePubSub(logger log.Logger, namespace string, timeSource clock.TimeSource) *executorStatePubSub {
	return &executorStatePubSub{
		subscribers: make(map[string]*executorStateSubscriber),
		logger:      logger,
		namespace:   namespace,
		timeSource:  timeSource,
	}
}

// Subscribe returns a channel that receives executor state updates.
func (p *executorStatePubSub) subscribe(ctx context.Context) (chan map[*store.ShardOwner][]string, func()) {
	uniqueID := uuid.New().String()
	subscriber := &executorStateSubscriber{
		updates: make(chan map[*store.ShardOwner][]string, 1),
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.subscribers[uniqueID] = subscriber

	unSub := func() {
		p.unSubscribe(uniqueID)
	}

	return subscriber.updates, unSub
}

func (p *executorStatePubSub) unSubscribe(uniqueID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.subscribers, uniqueID)
}

// Publish sends the state to all subscribers (non-blocking).
// If a subscriber already has a pending message, it is drained and replaced
// with the new state so the subscriber always sees the latest.
func (p *executorStatePubSub) publish(state map[*store.ShardOwner][]string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.timeSource.Now()
	var publishInterval time.Duration
	hasPublishInterval := p.hasPreviousPublish
	if hasPublishInterval {
		publishInterval = now.Sub(p.lastPublishedAt)
	}
	p.lastPublishedAt = now
	p.hasPreviousPublish = true

	for _, sub := range p.subscribers {
		select {
		case sub.updates <- state:
			sub.pendingUpdateSince = now
			sub.hasPendingUpdateSince = true
		default:
			p.logDroppedUpdate(sub, now, publishInterval, hasPublishInterval)

			// Preserve pendingUpdateSince when we drain the pending update ourselves.
			// Reset it if the consumer drained the update concurrently.
			select {
			case <-sub.updates:
			default:
				sub.pendingUpdateSince = now
			}
			sub.updates <- state
			if !sub.hasPendingUpdateSince {
				sub.pendingUpdateSince = now
				sub.hasPendingUpdateSince = true
			}
		}
	}
}

func (p *executorStatePubSub) logDroppedUpdate(
	sub *executorStateSubscriber,
	now time.Time,
	publishInterval time.Duration,
	hasPublishInterval bool,
) {
	logTags := []tag.Tag{tag.ShardNamespace(p.namespace)}
	if hasPublishInterval {
		logTags = append(logTags, tag.StateUpdatePublishInterval(publishInterval))
	}
	if sub.hasPendingUpdateSince {
		logTags = append(logTags, tag.SubscriberPendingUpdateDuration(now.Sub(sub.pendingUpdateSince)))
	}

	p.logger.Warn(
		"subscriber not keeping up, dropping intermediate state update and replacing with latest",
		logTags...,
	)
}
