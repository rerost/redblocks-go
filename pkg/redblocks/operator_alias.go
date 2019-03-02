package redblocks

import (
	"context"
	"time"
)

type aliasImp struct {
	store           Store
	rediskey        string
	notAvailableTTL time.Duration
}

func NewAliasSet(store Store, rediskey string, notAvailableTTL time.Duration) ComposedSet {
	return ComposeIDs(aliasImp{store: store, rediskey: rediskey, notAvailableTTL: notAvailableTTL}, store)
}

func (a aliasImp) KeySuffix() string {
	return ""
}

func (a aliasImp) Get(ctx context.Context) ([]IDWithScore, error) {
	return a.store.GetIDsWithScore(ctx, a.Key(), 0, -1)
}

func (a aliasImp) CacheTime() time.Duration {
	return time.Duration(0) // Do not use this value
}

func (a aliasImp) NotAvailableTTL() time.Duration {
	return a.notAvailableTTL
}

func (a aliasImp) Key() string {
	return a.rediskey
}

func (a aliasImp) Update(context.Context) error {
	return nil
}

func (s aliasImp) Available(ctx context.Context) (bool, error) {
	return s.store.Exists(ctx, s.Key())
}

func (a aliasImp) Warmup(ctx context.Context) error {
	return nil
}
