package redblocks

import (
	"context"
	"time"
)

type IDWithScore struct {
	ID    ID
	Score float64
}

type ID string

type Store interface {
	Save(ctx context.Context, key string, idsWithScore []IDWithScore, expire time.Duration) error
	GetIDs(ctx context.Context, key string, head int64, tail int64) ([]ID, error)
	GetIDsWithScore(ctx context.Context, key string, head int64, tail int64) ([]IDWithScore, error)
	Exists(ctx context.Context, key string) (bool, error)
	TTL(ctx context.Context, key string) (time.Duration, error)
	Interstore(ctx context.Context, dst string, expire time.Duration, keys ...string) error
	Unionstore(ctx context.Context, dst string, expire time.Duration, keys ...string) error
}
