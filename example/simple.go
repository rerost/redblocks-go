package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/rerost/redblocks-go/pkg/compose"
	"github.com/rerost/redblocks-go/pkg/operator"
	"github.com/rerost/redblocks-go/pkg/options"
	"github.com/rerost/redblocks-go/pkg/set"
	"github.com/rerost/redblocks-go/pkg/store"
)

func newPool() *redis.Pool {
	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 & time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	}
	return pool
}

func NewRegionSet(region string) set.Set {
	return regionSetImp{region}
}

type regionSetImp struct {
	region string
}

func (r regionSetImp) KeySuffix() string {
	return r.region
}

func (r regionSetImp) Get(ctx context.Context) ([]set.IDWithScore, error) {
	m := map[string][]set.IDWithScore{
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
	return time.Second * 10
}

func main() {
	ctx := context.Background()
	store := store.NewRedisStore(newPool())
	tokyo := compose.Compose(NewRegionSet("tokyo"), store)
	osaka := compose.Compose(NewRegionSet("osaka"), store)

	sets := operator.NewIntersectionSet(store, time.Second*100, tokyo, osaka)
	ids, err := sets.IDs(ctx, options.WithPagenation(0, -1))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println(ids)
}
