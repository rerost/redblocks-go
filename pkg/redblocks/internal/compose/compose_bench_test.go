package compose_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/rerost/redblocks-go/pkg/redblocks/internal/compose"
	"github.com/rerost/redblocks-go/pkg/redblocks/internal/operator"
	"github.com/rerost/redblocks-go/pkg/redblocks/internal/set"
	"github.com/rerost/redblocks-go/pkg/redblocks/internal/store"
)

func NewNumberSet(num int, testID string) set.Set {
	return numberSetImp{num: num, testID: testID}
}

type numberSetImp struct {
	num    int
	testID string
}

func (s numberSetImp) KeySuffix() string {
	return fmt.Sprintf("%d:%s", s.num, s.testID)
}

func (s numberSetImp) Get(ctx context.Context) ([]set.IDWithScore, error) {
	idsWithScore := make([]set.IDWithScore, 100, 100)
	for i := 0; i < 100; i++ {
		idsWithScore[i] = set.IDWithScore{ID: set.ID(fmt.Sprintf("%d", i)), Score: float64(i)}
	}

	return idsWithScore, nil
}

func (s numberSetImp) CacheTime() time.Duration {
	return time.Second * 2
}
func (s numberSetImp) NotAvailableTTL() time.Duration {
	return time.Microsecond * 500
}

func BenchmarkWarmup(b *testing.B) {
	store := store.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 & time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		composed := compose.Compose(NewNumberSet(i, b.Name()), store)
		composed.Warmup(ctx)
	}
}

func BenchmarkInterstoreWarmup(b *testing.B) {
	store := store.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 & time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})
	ctx := context.Background()
	composes := make([]compose.ComposedSet, 10, 10)
	for i := 0; i < 10; i++ {
		composes[i] = compose.Compose(NewNumberSet(i, b.Name()), store)
	}
	b.ResetTimer()
	intersection := operator.NewIntersectionSet(store, 10*time.Second, 1*time.Second, composes...)
	for i := 0; i < b.N; i++ {
		intersection.Warmup(ctx)
	}
}

func BenchmarkKey(b *testing.B) {
	store := store.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 & time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})
	compose := compose.Compose(NewNumberSet(1, b.Name()), store)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compose.Key()
	}
}
