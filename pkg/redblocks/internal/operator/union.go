package operator

import (
	"context"
	"strings"
	"time"

	"github.com/rerost/redblocks-go/pkg/redblocks/internal/compose"
	"github.com/rerost/redblocks-go/pkg/redblocks/internal/set"
	"github.com/rerost/redblocks-go/pkg/redblocks/internal/store"
	"github.com/srvc/fail"
)

type unionSetImp struct {
	store           store.Store
	sets            []compose.ComposedSet
	cacheTime       time.Duration
	notAvailableTTL time.Duration
}

func NewUnionSet(store store.Store, cacheTime time.Duration, notAvailableTTL time.Duration, sets ...compose.ComposedSet) compose.ComposedSet {
	return compose.ComposeIDs(compose.ComposeWarmup(NewUnionSetImp(store, cacheTime, notAvailableTTL, sets...), store), store)
}

func NewUnionSetImp(store store.Store, cacheTime time.Duration, notAvailableTTL time.Duration, sets ...compose.ComposedSet) compose.WithUpdate {
	return unionSetImp{
		store:           store,
		sets:            sets,
		cacheTime:       cacheTime,
		notAvailableTTL: notAvailableTTL,
	}
}

func (s unionSetImp) KeySuffix() string {
	return ""
}

func (s unionSetImp) Get(ctx context.Context) ([]set.IDWithScore, error) {
	err := s.Update(ctx)
	if err != nil {
		return []set.IDWithScore{}, fail.Wrap(err)
	}
	return s.store.GetIDsWithScore(ctx, s.Key(), 0, -1)
}

func (s unionSetImp) CacheTime() time.Duration {
	return s.cacheTime
}

func (s unionSetImp) NotAvailableTTL() time.Duration {
	return s.notAvailableTTL
}

func (s unionSetImp) Key() string {
	keys := make([]string, len(s.sets), len(s.sets))
	for i, set := range s.sets {
		keys[i] = set.Key()
	}
	return strings.Join(keys, "|")
}

func (s unionSetImp) Update(ctx context.Context) error {
	keys := make([]string, len(s.sets), len(s.sets))
	for i, set := range s.sets {
		keys[i] = set.Key()
	}
	for _, set := range s.sets {
		set.Warmup(ctx)
	}

	err := s.store.Unionstore(ctx, s.Key(), s.CacheTime(), keys...)
	if err != nil {
		return fail.Wrap(err)
	}
	return nil
}

func (c unionSetImp) Available(ctx context.Context) (bool, error) {
	exists, err := c.store.Exists(ctx, c.Key())
	if err != nil {
		return false, fail.Wrap(err)
	}
	if !exists {
		return false, nil
	}

	ttl, err := c.store.TTL(ctx, c.Key())
	if err != nil {
		return false, fail.Wrap(err)
	}
	if ttl < c.notAvailableTTL {
		return false, nil
	}

	return true, nil
}
