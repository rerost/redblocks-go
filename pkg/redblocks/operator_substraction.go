package redblocks

import (
	"context"
	"time"

	"github.com/srvc/fail"
)

type subtractionSetImp struct {
	store           Store
	set1            ComposedSet
	set2            ComposedSet
	cacheTime       time.Duration
	notAvailableTTL time.Duration
}

// NewSubtractionSet return set1 - set2
// WARING: This function is experimental.
// Because
// - Slow
// - set2's score needs to be much larger than set1' sscore
// - set2's score needs to be a negative value
func NewSubtractionSet(store Store, cacheTime time.Duration, notAvailableTTL time.Duration, set1 ComposedSet, set2 ComposedSet) ComposedSet {
	return ComposeIDs(ComposeWarmup(NewSubtractionSetImp(store, cacheTime, notAvailableTTL, set1, set2), store), store)
}

func NewSubtractionSetImp(store Store, cacheTime time.Duration, notAvailableTTL time.Duration, set1 ComposedSet, set2 ComposedSet) WithUpdate {
	return subtractionSetImp{
		store:           store,
		set1:            set1,
		set2:            set2,
		cacheTime:       cacheTime,
		notAvailableTTL: notAvailableTTL,
	}
}

func (s subtractionSetImp) KeySuffix() string {
	return ""
}

func (s subtractionSetImp) Get(ctx context.Context) ([]IDWithScore, error) {
	err := s.Update(ctx)
	if err != nil {
		return []IDWithScore{}, fail.Wrap(err)
	}
	return s.store.GetIDsWithScore(ctx, s.Key(), 0, -1, Asc)
}

func (s subtractionSetImp) CacheTime() time.Duration {
	return s.cacheTime
}
func (s subtractionSetImp) NotAvailableTTL() time.Duration {
	return s.notAvailableTTL
}

func (s subtractionSetImp) Key() string {
	return s.set1.Key() + "-" + s.set2.Key()
}

func (s subtractionSetImp) Update(ctx context.Context) error {
	if err := s.set1.Warmup(ctx); err != nil {
		return fail.Wrap(err)
	}
	if err := s.set2.Warmup(ctx); err != nil {
		return fail.Wrap(err)
	}

	err := s.store.Subtraction(ctx, s.Key(), s.CacheTime(), s.set1.Key(), s.set2.Key())
	if err != nil {
		return fail.Wrap(err)
	}

	return nil
}

func (c subtractionSetImp) Available(ctx context.Context) (bool, error) {
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
	if ttl < c.NotAvailableTTL() {
		return false, nil
	}

	return true, nil
}
