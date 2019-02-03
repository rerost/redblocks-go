package compose

import (
	"context"
	"reflect"

	"github.com/rerost/redblocks-go/pkg/set"
	"github.com/rerost/redblocks-go/pkg/store"
	"github.com/srvc/fail"
)

type WithWarmup interface {
	set.Set
	Key() string
	Warmup(ctx context.Context) error
}

type withWarmupImp struct {
	set.Set
	store store.Store
}

func ComposeWarmup(set set.Set, store store.Store) WithWarmup {
	return withWarmupImp{Set: set, store: store}
}

func (c withWarmupImp) Key() string {
	return reflect.TypeOf(c.Set).String() + ":" + c.Set.KeySuffix()
}

func (c withWarmupImp) Warmup(ctx context.Context) error {
	r, err := c.Get(ctx)
	if err != nil {
		return fail.Wrap(err)
	}

	return fail.Wrap(c.store.Save(ctx, c.Key(), r, c.CacheTime()))
}
