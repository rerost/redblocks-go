package compose

import (
	"context"
	"reflect"
	"time"

	"github.com/rerost/red-blocks-go/pkg/set"
	"github.com/rerost/red-blocks-go/pkg/store"
	"github.com/srvc/fail"
)

type WithWarmup interface {
	set.Set
	Key() string
	Warmup(ctx context.Context) error
}

type withWarmupImp struct {
	set   set.Set
	store store.Store
}

func ComposeWarmup(set set.Set, store store.Store) WithWarmup {
	return withWarmupImp{set: set, store: store}
}

func (c withWarmupImp) KeySuffix() string {
	return c.set.KeySuffix()
}
func (c withWarmupImp) Get(ctx context.Context) ([]set.IDsWithScore, error) {
	return c.set.Get(ctx)
}
func (c withWarmupImp) CacheTime() time.Duration {
	return c.set.CacheTime()
}

func (c withWarmupImp) Key() string {
	return reflect.TypeOf(c.set).String() + ":" + c.set.KeySuffix()
}

func (c withWarmupImp) Warmup(ctx context.Context) error {
	r, err := c.Get(ctx)
	if err != nil {
		return fail.Wrap(err)
	}

	return fail.Wrap(c.store.Save(ctx, c.Key(), r, c.CacheTime()))
}
