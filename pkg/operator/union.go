package operator

import (
	"context"
	"strings"
	"time"

	"github.com/rerost/redblocks-go/pkg/compose"
	"github.com/rerost/redblocks-go/pkg/set"
	"github.com/rerost/redblocks-go/pkg/store"
	"github.com/srvc/fail"
)

type unionSetImp struct {
	store     store.Store
	sets      []compose.ComposedSet
	cacheTime time.Duration
}

func NewUnionSet(store store.Store, cacheTime time.Duration, sets ...compose.ComposedSet) compose.ComposedSet {
	return compose.ComposeIDs(NewUnionSetImp(store, cacheTime, sets...), store)
}

func NewUnionSetImp(store store.Store, cacheTime time.Duration, sets ...compose.ComposedSet) compose.WithWarmup {
	return unionSetImp{
		store:     store,
		sets:      sets,
		cacheTime: cacheTime,
	}
}

func (s unionSetImp) KeySuffix() string {
	return ""
}

func (s unionSetImp) Get(ctx context.Context) ([]set.IDWithScore, error) {
	err := s.Warmup(ctx)
	if err != nil {
		return []set.IDWithScore{}, fail.Wrap(err)
	}
	return s.store.GetIDsWithScore(ctx, s.Key(), 0, -1)
}

func (s unionSetImp) CacheTime() time.Duration {
	return s.cacheTime
}

func (s unionSetImp) Key() string {
	keys := make([]string, len(s.sets), len(s.sets))
	for i, set := range s.sets {
		keys[i] = set.Key()
	}
	return strings.Join(keys, "|")
}

func (s unionSetImp) Warmup(ctx context.Context) error {
	keys := make([]string, len(s.sets), len(s.sets))
	for i, set := range s.sets {
		keys[i] = set.Key()
	}
	for _, set := range s.sets {
		set.Warmup(ctx)
	}

	err := s.store.Unionstore(ctx, s.Key(), keys...)
	if err != nil {
		return fail.Wrap(err)
	}
	return nil
}
