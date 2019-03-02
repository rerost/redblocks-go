package set

import (
	"context"
	"time"

	"github.com/rerost/redblocks-go/pkg/redblocks/internal/store"
)

type Set interface {
	KeySuffix() string
	Get(ctx context.Context) ([]IDWithScore, error)
	CacheTime() time.Duration
	NotAvailableTTL() time.Duration // NotAvailableTTL < CacheTime. For processing
}

type ID = store.ID
type IDWithScore = store.IDWithScore
