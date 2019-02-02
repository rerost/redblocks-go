package store

import (
	"context"
	"time"
)

type IDsWithScore struct {
	ID    ID
	Score float64
}

type ID string

type Store interface {
	Save(ctx context.Context, key string, idsWithScore []IDsWithScore, expire time.Duration) error
	GetIDs(ctx context.Context, key string, head int64, tail int64) ([]ID, error)
	GetIDsWithScore(ctx context.Context, key string, head int64, tail int64) ([]IDsWithScore, error)
	Exists(ctx context.Context, key string) (bool, error)
}
