package compose

import (
	"context"

	"github.com/rerost/redblocks-go/pkg/store"
	"github.com/srvc/fail"
)

type WithWarmup interface {
	WithUpdate
	Warmup(ctx context.Context) error
}

type withWarmupImp struct {
	WithUpdate
	store store.Store
}

func ComposeWarmup(withUpdate WithUpdate, store store.Store) WithWarmup {
	return withWarmupImp{WithUpdate: withUpdate, store: store}
}

func (c withWarmupImp) Warmup(ctx context.Context) error {
	available, err := c.Available(ctx)
	if err != nil {
		return fail.Wrap(err)
	}

	if !available {
		return fail.Wrap(c.Update(ctx))
	}
	return nil
}
