package redblocks

import (
	"context"

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

func (c withIDsImp) IDs(ctx context.Context, opts ...PagenationOption) ([]ID, error) {
	opt, err := PagenationOptionsToPagenationOption(opts)
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

	r, err := c.store.GetIDs(ctx, c.Key(), opt.Head, opt.Tail, opt.Order)
	if err != nil {
		return []ID{}, fail.Wrap(err)
	}

	return r, nil
}

func (c withIDsImp) IDsWithScore(ctx context.Context, opts ...PagenationOption) ([]IDWithScore, error) {
	opt, err := PagenationOptionsToPagenationOption(opts)
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

	r, err := c.store.GetIDsWithScore(ctx, c.Key(), opt.Head, opt.Tail, opt.Order)
	if err != nil {
		return []IDWithScore{}, fail.Wrap(err)
	}

	return r, nil
}

func (c withIDsImp) Count(ctx context.Context) (int64, error) {
	if err := c.Warmup(ctx); err != nil {
		return 0, fail.Wrap(err)
	}

	count, err := c.store.Count(ctx, c.Key())
	return count, fail.Wrap(err)
}
