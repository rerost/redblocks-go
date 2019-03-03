package redblocks

import (
	"context"
	"time"
)

type Set interface {
	KeySuffix() string
	Get(ctx context.Context) ([]IDWithScore, error)
	CacheTime() time.Duration
	NotAvailableTTL() time.Duration // NotAvailableTTL < CacheTime. For processing
}

type ComposedSet interface {
	Set
	Key() string
	Update(ctx context.Context) error
	Available(ctx context.Context) (bool, error)
	Warmup(ctx context.Context) error
	IDs(ctx context.Context, opts ...PagenationOption) ([]ID, error)
	IDsWithScore(ctx context.Context, opts ...PagenationOption) ([]IDWithScore, error)
	Count(ctx context.Context) (int64, error)
}

func Compose(wrapped Set, store Store) ComposedSet {
	return setToComposed(wrapped, store)
}

func setToComposed(set Set, store Store) ComposedSet {
	return ComposeIDs(ComposeWarmup(ComposeUpdate(set, store), store), store)
}
