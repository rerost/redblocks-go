package set

import (
	"time"

	"github.com/rerost/red-blocks-go/pkg/store"
)

type Set interface {
	KeySuffix() string
	Get() ([]IDsWithScore, error)
	CacheTime() time.Duration
}

type ID = store.ID
type IDsWithScore = store.IDsWithScore
