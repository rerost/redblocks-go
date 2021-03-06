package redblocks

import (
	"context"
	"reflect"

	"github.com/srvc/fail"
)

type WithUpdate interface {
	Set
	Key() string
	Update(ctx context.Context) error
	Available(ctx context.Context) (bool, error)
}

type withUpdateImp struct {
	Set
	store Store
}

func ComposeUpdate(set Set, store Store) WithUpdate {
	return withUpdateImp{Set: set, store: store}
}

func (c withUpdateImp) Key() string {
	return reflect.TypeOf(c.Set).String() + ":" + c.Set.KeySuffix()
}

func (c withUpdateImp) Update(ctx context.Context) error {
	r, err := c.Get(ctx)
	if err != nil {
		return fail.Wrap(err)
	}

	return fail.Wrap(c.store.Save(ctx, c.Key(), r, c.CacheTime()))
}

func (c withUpdateImp) Available(ctx context.Context) (bool, error) {
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
