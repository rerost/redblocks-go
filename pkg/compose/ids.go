package compose

import (
	"context"
	"time"

	"github.com/rerost/redblocks-go/pkg/options"
	"github.com/rerost/redblocks-go/pkg/set"
	"github.com/rerost/redblocks-go/pkg/store"
	"github.com/srvc/fail"
)

type WithIDs interface {
	WithWarmup
	IDs(ctx context.Context, opts ...options.PagenationOption) ([]set.ID, error)
	IDsWithScore(ctx context.Context, opts ...options.PagenationOption) ([]set.IDWithScore, error)
}

type withIDsImp struct {
	set   WithWarmup
	store store.Store
}

func ComposeIDs(set WithWarmup, store store.Store) WithIDs {
	return withIDsImp{set: set, store: store}
}

func (c withIDsImp) KeySuffix() string {
	return c.set.KeySuffix()
}
func (c withIDsImp) Get(ctx context.Context) ([]set.IDWithScore, error) {
	return c.set.Get(ctx)
}
func (c withIDsImp) CacheTime() time.Duration {
	return c.set.CacheTime()
}
func (c withIDsImp) Key() string {
	return c.set.Key()
}
func (c withIDsImp) Warmup(ctx context.Context) error {
	return c.set.Warmup(ctx)
}

func (c withIDsImp) IDs(ctx context.Context, opts ...options.PagenationOption) ([]set.ID, error) {
	opt, err := options.PagenationOptionsToPagenationOption(opts)
	if err != nil {
		return []set.ID{}, fail.Wrap(err)
	}

	exists, err := c.store.Exists(ctx, c.Key())
	if err != nil {
		return []set.ID{}, fail.Wrap(err)
	}
	if !exists {
		c.Warmup(ctx)
	}

	r, err := c.store.GetIDs(ctx, c.Key(), opt.Head, opt.Tail)
	if err != nil {
		return []store.ID{}, fail.Wrap(err)
	}

	return r, nil
}

func (c withIDsImp) IDsWithScore(ctx context.Context, opts ...options.PagenationOption) ([]store.IDWithScore, error) {
	opt, err := options.PagenationOptionsToPagenationOption(opts)
	if err != nil {
		return []store.IDWithScore{}, fail.Wrap(err)
	}

	exists, err := c.store.Exists(ctx, c.Key())
	if err != nil {
		return []store.IDWithScore{}, fail.Wrap(err)
	}
	if !exists {
		c.Warmup(ctx)
	}

	r, err := c.store.GetIDsWithScore(ctx, c.Key(), opt.Head, opt.Tail)
	if err != nil {
		return []store.IDWithScore{}, fail.Wrap(err)
	}

	return r, nil
}
