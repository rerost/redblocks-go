package set

import (
	"context"
	"reflect"
	"time"

	"github.com/rerost/red-blocks-go/pkg/options"
	"github.com/rerost/red-blocks-go/pkg/store"
	"github.com/srvc/fail"
)

type ComposedSet interface {
	KeySuffix() string
	Get() ([]IDsWithScore, error)
	CacheTime() time.Duration
	Key() string
	Warmup(ctx context.Context, opts ...options.Option) error
	IDs(ctx context.Context, opts ...options.Option) ([]ID, error)
	IDsWithScore(ctx context.Context, opts ...options.Option) ([]IDsWithScore, error)
}

func Compose(wrapped Set, store store.Store) ComposedSet {
	return composedSetImp{Set: wrapped, store: store}
}

type composedSetImp struct {
	Set
	store store.Store
}

func (c composedSetImp) Key() string {
	return reflect.TypeOf(c.Set).String() + ":" + c.Set.KeySuffix()
}

func (c composedSetImp) Warmup(ctx context.Context, opts ...options.Option) error {
	r, err := c.Get()
	if err != nil {
		return fail.Wrap(err)
	}

	return fail.Wrap(c.store.Save(ctx, c.Key(), r, c.CacheTime()))
}

func (c composedSetImp) IDs(ctx context.Context, opts ...options.Option) ([]ID, error) {
	opt, err := options.OptsToOption(opts)
	if err != nil {
		return []ID{}, fail.Wrap(err)
	}

	exists, err := c.store.Exists(ctx, c.Key())
	if err != nil {
		return []ID{}, fail.Wrap(err)
	}
	if !exists {
		c.Warmup(ctx, opt)
	}

	r, err := c.store.GetIDs(ctx, c.Key(), opt.Head, opt.Tail)
	if err != nil {
		return []ID{}, fail.Wrap(err)
	}

	return r, nil
}

func (c composedSetImp) IDsWithScore(ctx context.Context, opts ...options.Option) ([]IDsWithScore, error) {
	opt, err := options.OptsToOption(opts)
	if err != nil {
		return []IDsWithScore{}, fail.Wrap(err)
	}

	exists, err := c.store.Exists(ctx, c.Key())
	if err != nil {
		return []IDsWithScore{}, fail.Wrap(err)
	}
	if !exists {
		c.Warmup(ctx, opt)
	}

	r, err := c.store.GetIDsWithScore(ctx, c.Key(), opt.Head, opt.Tail)
	if err != nil {
		return []IDsWithScore{}, fail.Wrap(err)
	}

	return r, nil
}
