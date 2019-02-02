package set_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/google/go-cmp/cmp"
	"github.com/rerost/red-blocks-go/pkg/set"
	"github.com/rerost/red-blocks-go/pkg/store"
)

func NewRegionSet(region string) set.Set {
	return regionSetImp{region}
}

type regionSetImp struct {
	region string
}

func (r regionSetImp) KeySuffix() string {
	return r.region
}

func (r regionSetImp) Get() ([]set.IDsWithScore, error) {
	m := map[string][]set.IDsWithScore{
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
	}
	return m[r.region], nil
}

func (r regionSetImp) CacheTime() time.Duration {
	return time.Second * 100
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
	store := store.NewRedisStore(newPool())
	tokyo := set.Compose(NewRegionSet("tokyo"), store)
	osaka := set.Compose(NewRegionSet("osaka"), store)

	if diff := cmp.Diff(tokyo.Key(), osaka.Key()); diff == "" {
		t.Errorf("tokyo.Key and osaka.Key must be different")
	}

	ctx := context.Background()
	fmt.Println(tokyo.IDs(ctx))
	fmt.Println(osaka.IDsWithScore(ctx))
	// interstored := set.IntersectionSet(tokyo, osaka)
	// fmt.Println(interstored.IDs(ctx, options.WithPagenator(0, -1)))
}
