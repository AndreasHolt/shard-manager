package manager

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination=namespace_mock.go Namespace

import (
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/cache"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/config"
)

// Namespace is one configured namespace: its configuration and the components
// that serve it.
type Namespace interface {
	Name() string
	Config() config.Namespace
	Cache() cache.ShardCache
}

type namespace struct {
	config config.Namespace
	cache  cache.ShardCache
}

var _ Namespace = (*namespace)(nil)

// newNamespace bundles the components of one namespace.
func newNamespace(cfg config.Namespace, shardCache cache.ShardCache) *namespace {
	return &namespace{config: cfg, cache: shardCache}
}

func (n *namespace) Name() string             { return n.config.Name }
func (n *namespace) Config() config.Namespace { return n.config }
func (n *namespace) Cache() cache.ShardCache  { return n.cache }
