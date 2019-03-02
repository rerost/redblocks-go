package compose

import (
	"context"

	"github.com/rerost/redblocks-go/pkg/redblocks/internal/set"
	"github.com/rerost/redblocks-go/pkg/redblocks/internal/store"
	"github.com/rerost/redblocks-go/pkg/redblocks/options"
)

type ComposedSet interface {
	set.Set
	Key() string
	Update(ctx context.Context) error
	Available(ctx context.Context) (bool, error)
	Warmup(ctx context.Context) error
	IDs(ctx context.Context, opts ...options.PagenationOption) ([]store.ID, error)
	IDsWithScore(ctx context.Context, opts ...options.PagenationOption) ([]store.IDWithScore, error)
}

func Compose(wrapped set.Set, store store.Store) ComposedSet {
	return setToComposed(wrapped, store)
}

func setToComposed(set set.Set, store store.Store) ComposedSet {
	return ComposeIDs(ComposeWarmup(ComposeUpdate(set, store), store), store)
}
