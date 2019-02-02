package set

import (
	"context"
	"time"

	"github.com/rerost/red-blocks-go/pkg/store"
)

type Set interface {
	KeySuffix() string
	Get(ctx context.Context) ([]IDsWithScore, error)
	CacheTime() time.Duration
}

type ID = store.ID
type IDsWithScore = store.IDsWithScore
