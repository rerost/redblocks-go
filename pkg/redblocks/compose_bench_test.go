package redblocks_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/rerost/redblocks-go/pkg/redblocks"
)

func NewNumberSet(num int, testID string) redblocks.Set {
	return numberSetImp{num: num, testID: testID}
}

type numberSetImp struct {
	num    int
	testID string
}

func (s numberSetImp) KeySuffix() string {
	return fmt.Sprintf("%d:%s", s.num, s.testID)
}

func (s numberSetImp) Get(ctx context.Context) ([]redblocks.IDWithScore, error) {
	idsWithScore := make([]redblocks.IDWithScore, 100, 100)
	for i := 0; i < 100; i++ {
		idsWithScore[i] = redblocks.IDWithScore{ID: redblocks.ID(fmt.Sprintf("%d", i)), Score: float64(i)}
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
	store := redblocks.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 & time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		composed := redblocks.Compose(NewNumberSet(i, b.Name()), store)
		composed.Warmup(ctx)
	}
}

func BenchmarkInterstoreWarmup(b *testing.B) {
	store := redblocks.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 & time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})
	ctx := context.Background()
	composes := make([]redblocks.ComposedSet, 10, 10)
	for i := 0; i < 10; i++ {
		composes[i] = redblocks.Compose(NewNumberSet(i, b.Name()), store)
	}
	b.ResetTimer()
	intersection := redblocks.NewIntersectionSet(store, 10*time.Second, 1*time.Second, []float64{1, 1}, redblocks.Sum, composes...)
	for i := 0; i < b.N; i++ {
		intersection.Warmup(ctx)
	}
}

func BenchmarkKey(b *testing.B) {
	store := redblocks.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 & time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})
	compose := redblocks.Compose(NewNumberSet(1, b.Name()), store)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compose.Key()
	}
}
