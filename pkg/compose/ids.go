package compose

import (
	"context"

	"github.com/rerost/redblocks-go/pkg/options"
	"github.com/rerost/redblocks-go/pkg/set"
	"github.com/rerost/redblocks-go/pkg/store"
	"github.com/srvc/fail"
)

type WithIDs = ComposedSet

type withIDsImp struct {
	WithWarmup
	store store.Store
}

func ComposeIDs(set WithWarmup, store store.Store) WithIDs {
	return withIDsImp{WithWarmup: set, store: store}
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
