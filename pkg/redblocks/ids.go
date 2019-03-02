package redblocks

import (
	"context"

	"github.com/rerost/redblocks-go/pkg/options"
	"github.com/srvc/fail"
)

type WithIDs = ComposedSet

type withIDsImp struct {
	WithWarmup
	store Store
}

func ComposeIDs(set WithWarmup, store Store) WithIDs {
	return withIDsImp{WithWarmup: set, store: store}
}

func (c withIDsImp) IDs(ctx context.Context, opts ...options.PagenationOption) ([]ID, error) {
	opt, err := options.PagenationOptionsToPagenationOption(opts)
	if err != nil {
		return []ID{}, fail.Wrap(err)
	}

	exists, err := c.store.Exists(ctx, c.Key())
	if err != nil {
		return []ID{}, fail.Wrap(err)
	}
	if !exists {
		c.Warmup(ctx)
	}

	r, err := c.store.GetIDs(ctx, c.Key(), opt.Head, opt.Tail)
	if err != nil {
		return []ID{}, fail.Wrap(err)
	}

	return r, nil
}

func (c withIDsImp) IDsWithScore(ctx context.Context, opts ...options.PagenationOption) ([]IDWithScore, error) {
	opt, err := options.PagenationOptionsToPagenationOption(opts)
	if err != nil {
		return []IDWithScore{}, fail.Wrap(err)
	}

	exists, err := c.store.Exists(ctx, c.Key())
	if err != nil {
		return []IDWithScore{}, fail.Wrap(err)
	}
	if !exists {
		c.Warmup(ctx)
	}

	r, err := c.store.GetIDsWithScore(ctx, c.Key(), opt.Head, opt.Tail)
	if err != nil {
		return []IDWithScore{}, fail.Wrap(err)
	}

	return r, nil
}
