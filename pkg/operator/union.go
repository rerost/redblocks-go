package operator

import (
	"context"
	"strings"
	"time"

	"github.com/rerost/red-blocks-go/pkg/compose"
	"github.com/rerost/red-blocks-go/pkg/set"
	"github.com/rerost/red-blocks-go/pkg/store"
	"github.com/srvc/fail"
)

type unionSetImp struct {
	store     store.Store
	sets      []compose.ComposedSet
	cacheTime time.Duration
}

func NewUnionSet(store store.Store, cacheTime time.Duration, sets ...compose.ComposedSet) compose.ComposedSet {
	return compose.ComposeIDs(NewIntersectionSetImp(store, cacheTime, sets...), store)
}

func NewUnionSetImp(store store.Store, cacheTime time.Duration, sets ...compose.ComposedSet) compose.WithWarmup {
	return intersectionSetImp{
		store:     store,
		sets:      sets,
		cacheTime: cacheTime,
	}
}

func (s intersectionSetImp) KeySuffix() string {
	return ""
}

func (s intersectionSetImp) Get(ctx context.Context) ([]set.IDsWithScore, error) {
	err := s.Warmup(ctx)
	if err != nil {
		return []set.IDsWithScore{}, fail.Wrap(err)
	}
	return s.store.GetIDsWithScore(ctx, s.Key(), 0, -1)
}

func (s intersectionSetImp) CacheTime() time.Duration {
	return s.cacheTime
}

func (s intersectionSetImp) Key() string {
	keys := make([]string, len(s.sets), len(s.sets))
	for i, set := range s.sets {
		keys[i] = set.Key()
	}
	return strings.Join(keys, "|")
}

func (s intersectionSetImp) Warmup(ctx context.Context) error {
	keys := make([]string, len(s.sets), len(s.sets))
	for i, set := range s.sets {
		keys[i] = set.Key()
	}

	err := s.store.Unionstore(ctx, s.Key(), keys...)
	if err != nil {
		return fail.Wrap(err)
	}
	return nil
}
