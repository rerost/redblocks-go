package operator

import (
	"context"
	"time"

	"github.com/rerost/red-blocks-go/pkg/set"
	"github.com/rerost/red-blocks-go/pkg/store"
)

type intersectionSetImp struct {
	store     store.Store
	set1      set.ComposedSet
	set2      set.ComposedSet
	cacheTime time.Duration
}

func NewIntersectionSet(store store.Store, set1 set.ComposedSet, set2 set.ComposedSet, cacheTime time.Duration) set.ComposedSet {
	return intersectionSetImp{
		store:     stroe,
		set1:      set1,
		set2:      set2,
		cacheTime: cacheTime,
	}
}

// type ComposedSet interface {
// 	KeySuffix() string
// 	Get() ([]IDsWithScore, error)
// 	CacheTime() time.Duration
// 	Key() string
// 	Warmup(ctx context.Context, opts ...options.PagenationOption) error
// 	IDs(ctx context.Context, opts ...options.PagenationOption) ([]ID, error)
// 	IDsWithScore(ctx context.Context, opts ...options.PagenationOption) ([]IDsWithScore, error)
// }

func (s intersectionSetImp) KeySuffix() string {
	return ""
}

func (s intersectionSetImp) Get(ctx context.Context) ([]set.IDsWithScore, error) {
}
