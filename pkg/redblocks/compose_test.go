package redblocks_test

import (
	"context"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/google/go-cmp/cmp"
	"github.com/rerost/redblocks-go/pkg/redblocks"
)

func NewRegionSet(region string) redblocks.Set {
	return regionSetImp{region}
}

type regionSetImp struct {
	region string
}

func (r regionSetImp) KeySuffix() string {
	return r.region
}

func (r regionSetImp) Get(ctx context.Context) ([]redblocks.IDWithScore, error) {
	m := map[string][]redblocks.IDWithScore{
		"tokyo": {
			{
				ID: "test1",
			},
			{
				ID: "test2",
			},
			{
				ID: "test3",
			},
		},
		"osaka": {
			{
				ID: "test1",
			},
			{
				ID: "test2",
			},
			{
				ID: "test3",
			},
			{
				ID: "test4",
			},
		},
		"donotshow": {
			{
				ID:    "test1",
				Score: -100,
			},
			{
				ID:    "test2",
				Score: -100,
			},
		},
	}
	return m[r.region], nil
}

func (r regionSetImp) CacheTime() time.Duration {
	return time.Second * 100
}

func (r regionSetImp) NotAvailableTTL() time.Duration {
	return time.Second * 10
}

func newPool() *redis.Pool {
	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 & time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	}
	return pool
}

func TestCreateRegion(t *testing.T) {
	store := redblocks.NewRedisStore(newPool())
	tokyo := redblocks.Compose(NewRegionSet("tokyo"), store)
	osaka := redblocks.Compose(NewRegionSet("osaka"), store)

	if diff := cmp.Diff(tokyo.Key(), osaka.Key()); diff == "" {
		t.Errorf("tokyo.Key and osaka.Key must be different")
	}

	ctx := context.Background()
	ids, err := tokyo.IDs(ctx, redblocks.WithPagenation(0, -1))
	if err != nil {
		t.Error(err)
	}
	if diff := cmp.Diff(ids, []redblocks.ID{"test1", "test2", "test3"}); diff != "" {
		t.Errorf(diff)
	}
}

func TestIntersection(t *testing.T) {
	store := redblocks.NewRedisStore(newPool())
	tokyo := redblocks.Compose(NewRegionSet("tokyo"), store)
	osaka := redblocks.Compose(NewRegionSet("osaka"), store)

	ctx := context.Background()
	interstored := redblocks.NewIntersectionSet(store, time.Second*100, time.Second*10, []float64{1, 1}, redblocks.Sum, tokyo, osaka)
	interstoredResult, err := interstored.IDsWithScore(ctx)
	if err != nil {
		t.Error(err)
	}

	tokyoResult, err := tokyo.IDsWithScore(ctx)
	if err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(interstoredResult, tokyoResult); diff != "" {
		t.Errorf(diff)
	}
}

func TesUnion(t *testing.T) {
	store := redblocks.NewRedisStore(newPool())
	tokyo := redblocks.Compose(NewRegionSet("tokyo"), store)
	osaka := redblocks.Compose(NewRegionSet("osaka"), store)

	ctx := context.Background()
	interstored := redblocks.NewUnionSet(store, time.Second*100, time.Second*10, []float64{1, 1}, redblocks.Sum, tokyo, osaka)
	interstoredResult, err := interstored.IDsWithScore(ctx)
	if err != nil {
		t.Error(err)
	}

	osakaResult, err := osaka.IDsWithScore(ctx)
	if err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(interstoredResult, osakaResult); diff != "" {
		t.Errorf(diff)
	}
}

func TestSubtraction(t *testing.T) {
	store := redblocks.NewRedisStore(newPool())
	donotshow := redblocks.Compose(NewRegionSet("donotshow"), store)
	tokyo := redblocks.Compose(NewRegionSet("tokyo"), store)

	ctx := context.Background()
	subtracted := redblocks.NewSubtractionSet(store, time.Second*100, time.Second*10, tokyo, donotshow)
	subtractedResult, err := subtracted.IDsWithScore(ctx)
	if err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(subtractedResult, []redblocks.IDWithScore{{ID: "test3", Score: 0}}); diff != "" {
		t.Errorf(diff)
	}
}

func TestWithoutRedis(t *testing.T) {
	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 & time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6380") },
	}
	store := redblocks.NewRedisStore(pool)
	tokyo := redblocks.Compose(NewRegionSet("tokyo"), store)
	osaka := redblocks.Compose(NewRegionSet("osaka"), store)
	ctx := context.Background()
	_, err := tokyo.IDs(ctx)
	if err == nil {
		t.Errorf("Expected not nil")
	}

	err = tokyo.Warmup(ctx)
	if err == nil {
		t.Errorf("Expected not nil")
	}

	interstored := redblocks.NewIntersectionSet(store, 10*time.Second, time.Second, []float64{1, 1}, redblocks.Sum, tokyo, osaka)
	_, err = interstored.IDsWithScore(ctx)
	if err == nil {
		t.Errorf("Expected not nil")
	}
}
