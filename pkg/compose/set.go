package compose

import (
	"context"

	"github.com/rerost/red-blocks-go/pkg/options"
	"github.com/rerost/red-blocks-go/pkg/set"
	"github.com/rerost/red-blocks-go/pkg/store"
)

type ComposedSet interface {
	set.Set
	Key() string
	Warmup(ctx context.Context) error
	IDs(ctx context.Context, opts ...options.PagenationOption) ([]store.ID, error)
	IDsWithScore(ctx context.Context, opts ...options.PagenationOption) ([]store.IDsWithScore, error)
}

func Compose(wrapped set.Set, store store.Store) ComposedSet {
	return setToComposed(wrapped, store)
}

func setToComposed(set set.Set, store store.Store) ComposedSet {
	return ComposeIDs(ComposeWarmup(set, store), store)
}
