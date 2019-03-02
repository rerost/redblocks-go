package redblocks

import (
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/rerost/redblocks-go/pkg/redblocks/internal/compose"
	"github.com/rerost/redblocks-go/pkg/redblocks/internal/operator"
	"github.com/rerost/redblocks-go/pkg/redblocks/internal/set"
	"github.com/rerost/redblocks-go/pkg/redblocks/internal/store"
)

var NewRedisStore func(pool *redis.Pool) store.Store

var Compose func(wrapped set.Set, store store.Store) compose.ComposedSet

var NewIntersectionSet func(store store.Store, cacheTime time.Duration, notAvailableTTL time.Duration, sets ...compose.ComposedSet) compose.ComposedSet
var NewUnionSet func(store store.Store, cacheTime time.Duration, notAvailableTTL time.Duration, sets ...compose.ComposedSet) compose.ComposedSet
var NewAliasSet func(store store.Store, rediskeys string, notAvailableTTL time.Duration) compose.ComposedSet

type IDWithScore = store.IDWithScore
type ID = store.ID
type Set = set.Set
type ComposedSet = compose.ComposedSet
type Store = store.Store

func init() {
	NewRedisStore = store.NewRedisStore

	Compose = compose.Compose

	NewIntersectionSet = operator.NewIntersectionSet
	NewUnionSet = operator.NewUnionSet
	NewAliasSet = operator.NewAliasSet
}
