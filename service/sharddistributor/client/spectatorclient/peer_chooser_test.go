package spectatorclient

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"go.uber.org/yarpc/api/peer"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/peer/hostport"
	"go.uber.org/yarpc/transport/grpc"
	"go.uber.org/yarpc/yarpcerrors"
	"go.uber.org/zap"

	"github.com/cadence-workflow/shard-manager/common/clock"
	"github.com/cadence-workflow/shard-manager/common/types"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/client/clientcommon"
)

type peerChooserTestData struct {
	chooser       *SpectatorPeerChooser
	transport     *grpc.Transport
	timeSource    clock.MockedTimeSource
	mockSpectator *MockSpectator
	namespace     string
	peerTTL       time.Duration
}

func TestSpectatorPeerChooser_Choose_MissingShardKey(t *testing.T) {
	td := newPeerChooserTestData(t)

	req := &transport.Request{
		ShardKey: "",
		Headers:  transport.NewHeaders(),
	}

	p, onFinish, err := td.chooser.Choose(context.Background(), req)

	assert.Error(t, err)
	assert.Nil(t, p)
	assert.Nil(t, onFinish)
	assert.Contains(t, err.Error(), "ShardKey")
}

func TestSpectatorPeerChooser_Choose_MissingNamespaceHeader(t *testing.T) {
	td := newPeerChooserTestData(t)

	req := &transport.Request{
		ShardKey: "shard-1",
		Headers:  transport.NewHeaders(),
	}

	p, onFinish, err := td.chooser.Choose(context.Background(), req)

	assert.Error(t, err)
	assert.Nil(t, p)
	assert.Nil(t, onFinish)
	assert.Contains(t, err.Error(), "x-shard-distributor-namespace")
}

func TestSpectatorPeerChooser_Choose_SpectatorNotFound(t *testing.T) {
	td := newPeerChooserTestData(t)

	req := &transport.Request{
		ShardKey: "shard-1",
		Headers:  transport.NewHeaders().With(NamespaceHeader, "unknown-namespace"),
	}

	p, onFinish, err := td.chooser.Choose(context.Background(), req)

	assert.Error(t, err)
	assert.Nil(t, p)
	assert.Nil(t, onFinish)
	assert.Contains(t, err.Error(), "spectator not found")
}

func TestSpectatorPeerChooser_Lifecycle(t *testing.T) {
	tests := []struct {
		name   string
		owners int
	}{
		{
			name:   "stop by a single owner releases cached peers",
			owners: 1,
		},
		{
			name:   "concurrent start and stop by several owners release cached peers",
			owners: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Registered before the fixture, so it runs after the fixture's cleanup stops the transport.
			t.Cleanup(func() { goleak.VerifyNone(t) })
			td := newPeerChooserTestData(t)

			shardKey := "shard-1"
			cachedPeerAddress := "127.0.0.1:7953"
			td.expectShardOwner(shardKey, cachedPeerAddress)

			callConcurrently := func(lifecycleMethod func() error) {
				var wg sync.WaitGroup
				for range tc.owners {
					wg.Add(1)
					go func() {
						defer wg.Done()
						assert.NoError(t, lifecycleMethod())
					}()
				}
				wg.Wait()
			}

			assert.False(t, td.chooser.IsRunning(), "a new chooser should not be running before Start")

			callConcurrently(td.chooser.Start)
			assert.True(t, td.chooser.IsRunning(), "chooser should be running after Start")
			// The eviction loop's ticker is the clock's only waiter.
			td.timeSource.BlockUntil(1)

			req := &transport.Request{
				ShardKey: shardKey,
				Headers:  transport.NewHeaders().With(NamespaceHeader, td.namespace),
			}
			chosenPeer, _, err := td.chooser.Choose(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, cachedPeerAddress, chosenPeer.Identifier(), "Choose should route to the shard owner's address")

			callConcurrently(td.chooser.Stop)
			assert.False(t, td.chooser.IsRunning(), "chooser should not be running after Stop")

			// Releasing again fails only if the chooser already released its reference.
			err = td.transport.ReleasePeer(hostport.Identify(cachedPeerAddress), &noOpSubscriber{})
			var noReferenceErr peer.ErrTransportHasNoReferenceToPeer
			assert.ErrorAs(t, err, &noReferenceErr, "Stop should release the chooser's reference to every cached peer")
		})
	}
}

func TestSpectatorPeerChooser_SetSpectators(t *testing.T) {
	td := newPeerChooserTestData(t)

	spectators := &Spectators{spectators: make(map[string]Spectator)}
	td.chooser.SetSpectators(spectators)

	assert.Equal(t, spectators, td.chooser.spectators)
}

func TestSpectatorPeerChooser_Choose_Success(t *testing.T) {
	td := newPeerChooserTestData(t)

	req := &transport.Request{
		ShardKey: "shard-1",
		Headers:  transport.NewHeaders().With(NamespaceHeader, td.namespace),
	}
	td.expectShardOwner("shard-1", "127.0.0.1:7953")

	p, onFinish, err := td.chooser.Choose(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, p)
	assert.NotNil(t, onFinish)
	assert.Equal(t, "127.0.0.1:7953", p.Identifier())
	assert.Len(t, td.chooser.peers, 1)
	assert.Equal(t, "127.0.0.1:7953", td.chooser.peers["127.0.0.1:7953"].peer.Identifier())
}

// Routing a drained shard must fail with a code callers do not retry
func TestSpectatorPeerChooser_Choose_DrainedShardIsNotRetryable(t *testing.T) {
	td := newPeerChooserTestData(t)

	req := &transport.Request{
		ShardKey: "shard-1",
		Headers:  transport.NewHeaders().With(NamespaceHeader, td.namespace),
	}

	td.mockSpectator.EXPECT().
		GetShardOwner(gomock.Any(), "shard-1").
		Return(nil, &types.ShardDrainedError{Namespace: td.namespace, ShardKey: "shard-1"})

	p, onFinish, err := td.chooser.Choose(context.Background(), req)

	require.Error(t, err)
	assert.Nil(t, p)
	assert.Nil(t, onFinish)
	assert.Equal(t, yarpcerrors.CodeFailedPrecondition, yarpcerrors.FromError(err).Code())
	assert.Empty(t, td.chooser.peers, "a drained shard must not create a peer")
}

func TestSpectatorPeerChooser_Choose_ReusesPeer(t *testing.T) {
	td := newPeerChooserTestData(t)

	req := &transport.Request{
		ShardKey: "shard-1",
		Headers:  transport.NewHeaders().With(NamespaceHeader, td.namespace),
	}
	td.expectShardOwner("shard-1", "127.0.0.1:7953").Times(2)

	// First call creates the peer
	firstPeer, _, err := td.chooser.Choose(context.Background(), req)
	require.NoError(t, err)

	// Second call should reuse the same peer
	secondPeer, _, err := td.chooser.Choose(context.Background(), req)

	assert.NoError(t, err)
	assert.Equal(t, firstPeer, secondPeer)
	assert.Len(t, td.chooser.peers, 1)
}

func TestSpectatorPeerChooser_Choose_TracksLastUsed(t *testing.T) {
	td := newPeerChooserTestData(t)

	req := &transport.Request{
		ShardKey: "shard-1",
		Headers:  transport.NewHeaders().With(NamespaceHeader, td.namespace),
	}
	td.expectShardOwner("shard-1", "127.0.0.1:7953").Times(2)

	// First call — creates peer, sets lastUsed to t0
	_, _, err := td.chooser.Choose(context.Background(), req)
	require.NoError(t, err)
	firstLastUsed := td.chooser.peers["127.0.0.1:7953"].lastUsed

	td.timeSource.Advance(30 * time.Second)

	// Second call — reuses peer, should update lastUsed to t0+30s
	_, _, err = td.chooser.Choose(context.Background(), req)
	require.NoError(t, err)
	secondLastUsed := td.chooser.peers["127.0.0.1:7953"].lastUsed

	assert.True(t, secondLastUsed.After(firstLastUsed), "lastUsed should be updated on reuse")
}

func TestSpectatorPeerChooser_EvictStalePeers(t *testing.T) {
	tests := []struct {
		name              string
		idleOffsetFromTTL time.Duration
		expectEvicted     bool
	}{
		{
			name:              "peer within TTL is kept",
			idleOffsetFromTTL: -time.Minute,
			expectEvicted:     false,
		},
		{
			name:              "peer exactly at TTL boundary is kept",
			idleOffsetFromTTL: 0,
			expectEvicted:     false,
		},
		{
			name:              "peer past TTL is evicted",
			idleOffsetFromTTL: time.Millisecond,
			expectEvicted:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			td := newPeerChooserTestData(t)

			// Manually insert a tracked peer with lastUsed = now
			p, err := td.transport.RetainPeer(hostport.Identify("127.0.0.1:7953"), &noOpSubscriber{})
			require.NoError(t, err)
			td.chooser.peers["127.0.0.1:7953"] = &trackedPeer{
				peer:     p,
				lastUsed: td.timeSource.Now(),
			}

			td.timeSource.Advance(td.peerTTL + tc.idleOffsetFromTTL)

			// Run eviction directly
			td.chooser.evictStalePeers()

			if tc.expectEvicted {
				assert.Empty(t, td.chooser.peers)
			} else {
				assert.Len(t, td.chooser.peers, 1)
			}
		})
	}
}

// newPeerChooserTestData builds a chooser the way production does, on a started
// gRPC transport and a mocked clock. Requests for the returned namespace are
// routed through mockSpectator.
func newPeerChooserTestData(t *testing.T) *peerChooserTestData {
	t.Helper()

	peerTransport := grpc.NewTransport()
	require.NoError(t, peerTransport.Start())
	t.Cleanup(func() { assert.NoError(t, peerTransport.Stop()) })

	timeSource := clock.NewMockedTimeSource()
	peerTTL := 2 * time.Minute
	params := SpectatorPeerChooserParams{
		Transport:  peerTransport,
		Logger:     zap.NewNop(),
		Config:     clientcommon.Config{PeerTTL: peerTTL},
		TimeSource: timeSource,
	}
	chooser := NewSpectatorPeerChooser(params).(*SpectatorPeerChooser)

	namespace := "test-namespace"
	mockSpectator := NewMockSpectator(gomock.NewController(t))
	chooser.SetSpectators(&Spectators{spectators: map[string]Spectator{namespace: mockSpectator}})

	return &peerChooserTestData{
		chooser:       chooser,
		transport:     peerTransport,
		timeSource:    timeSource,
		mockSpectator: mockSpectator,
		namespace:     namespace,
		peerTTL:       peerTTL,
	}
}

// expectShardOwner makes the spectator report the executor at grpcAddress as
// the owner of shardKey.
func (td *peerChooserTestData) expectShardOwner(shardKey, grpcAddress string) *gomock.Call {
	owner := &ShardOwner{
		ExecutorID: "executor-1",
		Metadata:   map[string]string{clientcommon.GrpcAddressMetadataKey: grpcAddress},
	}
	return td.mockSpectator.EXPECT().GetShardOwner(gomock.Any(), shardKey).Return(owner, nil)
}
