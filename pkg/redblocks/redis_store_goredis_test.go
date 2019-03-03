package redblocks_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/google/go-cmp/cmp"
	"github.com/rerost/redblocks-go/pkg/redblocks"
)

var redisdb *redis.Client

func init() {
	redisdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
}

func TestGorediStoreGetIDs(t *testing.T) {
	ctx := context.Background()
	redisStore := redblocks.NewGoredisStore(redisdb.WithContext)
	key := "TestGorediStoreGetIDs"
	err := redisStore.Save(ctx, key, []redblocks.IDWithScore{
		{
			ID:    "1",
			Score: 1,
		},
		{
			ID:    "2",
			Score: 2,
		},
	}, 100*time.Second)
	if err != nil {
		t.Error(err)
	}

	ids, err := redisStore.GetIDs(ctx, key, 0, -1, redblocks.Asc)
	if err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(ids, []redblocks.ID{"1", "2"}); diff != "" {
		t.Errorf(diff)
	}
}

func TestGorediStoreGetIDsWithScore(t *testing.T) {
	redisStore := redblocks.NewGoredisStore(redisdb.WithContext)
	key := "TestGorediStoreGetIDsWithScore"
	ctx := context.Background()

	idsWithScore := []redblocks.IDWithScore{
		{
			ID:    "1",
			Score: 1,
		},
		{
			ID:    "2",
			Score: 2,
		},
	}

	err := redisStore.Save(ctx, key, idsWithScore, 100*time.Second)
	if err != nil {
		t.Error(err)
	}

	result, err := redisStore.GetIDsWithScore(ctx, key, 0, -1, redblocks.Asc)
	if err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(result, idsWithScore); diff != "" {
		t.Errorf(diff)
	}
}

func TestGoRedisStoreExists(t *testing.T) {
	redisStore := redblocks.NewGoredisStore(redisdb.WithContext)
	key := "TestGoRedisStoreExists"
	ctx := context.Background()

	idsWithScore := []redblocks.IDWithScore{
		{
			ID:    "1",
			Score: 1,
		},
		{
			ID:    "2",
			Score: 2,
		},
	}

	err := redisStore.Save(ctx, key, idsWithScore, 100*time.Second)
	if err != nil {
		t.Error(err)
	}

	result, err := redisStore.Exists(ctx, key)
	if err != nil {
		t.Error(err)
	}
	if diff := cmp.Diff(result, true); diff != "" {
		t.Errorf(diff)
	}
}

func TestGorediStoreTTL(t *testing.T) {
	redisStore := redblocks.NewGoredisStore(redisdb.WithContext)
	key := "TestGoRedisStoreTTL"

	ctx := context.Background()

	cacheTime := time.Second * 100
	err := redisStore.Save(ctx, key, []redblocks.IDWithScore{{ID: "1"}}, cacheTime)
	if err != nil {
		t.Error(err)
	}

	ttl, err := redisStore.TTL(ctx, key)
	if err != nil {
		t.Error(err)
	}
	if !(0 < ttl && ttl <= cacheTime) {
		t.Errorf("want: 0 < ttl < cacheTime but ttl: %v", ttl)
	}

	emptyKey := key + ":" + "EMPTY"
	_, err = redisStore.TTL(ctx, emptyKey)
	if diff := cmp.Diff(err.Error(), "Not found"); diff != "" {
		t.Errorf(diff)
	}

	notExpireKey := key + ":" + "NOT_EXPIRE"
	cmd := redisdb.Set(notExpireKey, notExpireKey, 0)
	if err := cmd.Err(); err != nil {
		t.Error(err)
	}
	_, err = redisStore.TTL(ctx, notExpireKey)
	if diff := cmp.Diff(err.Error(), "Not configured expire"); diff != "" {
		t.Error(diff)
	}

	redisdb.Del(notExpireKey)
}

func TestGoRedisStoreInterstore(t *testing.T) {
	redisStore := redblocks.NewGoredisStore(redisdb.WithContext)
	key := "TestGoRedisStoreInterstore"
	key1 := "TestGoRedisStoreInterstore1"
	key2 := "TestGoRedisStoreInterstore2"
	ctx := context.Background()

	idsWithScore1 := []redblocks.IDWithScore{
		{
			ID:    "1",
			Score: 1,
		},
		{
			ID:    "2",
			Score: 2,
		},
	}

	idsWithScore2 := []redblocks.IDWithScore{
		{
			ID:    "1",
			Score: 1,
		},
		{
			ID:    "2",
			Score: 2,
		},
		{
			ID:    "3",
			Score: 3,
		},
	}

	err := redisStore.Save(ctx, key1, idsWithScore1, 100*time.Second)
	if err != nil {
		t.Error(err)
	}

	err = redisStore.Save(ctx, key2, idsWithScore2, 100*time.Second)
	if err != nil {
		t.Error(err)
	}

	err = redisStore.Interstore(ctx, key, time.Second, []float64{1, 1}, redblocks.Sum, key1, key2)
	if err != nil {
		t.Error(err)
	}
	result, err := redisStore.GetIDsWithScore(ctx, key, 0, -1, redblocks.Asc)
	if err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(result, []redblocks.IDWithScore{{ID: "1", Score: 2}, {ID: "2", Score: 4}}); diff != "" {
		t.Errorf(diff)
	}
}

func TestGoRedisStoreUnionstore(t *testing.T) {
	redisStore := redblocks.NewGoredisStore(redisdb.WithContext)
	key := "TestGoRedisStoreUnionstore"
	key1 := "TestGoRedisStoreUnionstore1"
	key2 := "TestGoRedisStoreUnionstore2"
	ctx := context.Background()

	idsWithScore1 := []redblocks.IDWithScore{
		{
			ID:    "1",
			Score: 1,
		},
		{
			ID:    "2",
			Score: 2,
		},
	}

	idsWithScore2 := []redblocks.IDWithScore{
		{
			ID:    "1",
			Score: 1,
		},
		{
			ID:    "2",
			Score: 2,
		},
		{
			ID:    "3",
			Score: 3,
		},
	}

	err := redisStore.Save(ctx, key1, idsWithScore1, 100*time.Second)
	if err != nil {
		t.Error(err)
	}

	err = redisStore.Save(ctx, key2, idsWithScore2, 100*time.Second)
	if err != nil {
		t.Error(err)
	}

	err = redisStore.Unionstore(ctx, key, time.Second, []float64{1, 1}, redblocks.Sum, key1, key2)
	if err != nil {
		t.Error(err)
	}
	result, err := redisStore.GetIDsWithScore(ctx, key, 0, -1, redblocks.Asc)
	if err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(result, []redblocks.IDWithScore{{ID: "1", Score: 2}, {ID: "3", Score: 3}, {ID: "2", Score: 4}}); diff != "" {
		t.Errorf(diff)
	}
}

func TestGoRedisStoreSubtraction(t *testing.T) {
	redisStore := redblocks.NewGoredisStore(redisdb.WithContext)
	key := "TestGoRedisStoreSubtraction"
	key1 := "TestGoRedisStoreSubtraction1"
	key2 := "TestGoRedisStoreSubtraction2"
	ctx := context.Background()

	idsWithScore1 := []redblocks.IDWithScore{
		{
			ID:    "1",
			Score: 1,
		},
		{
			ID:    "2",
			Score: 2,
		},
		{
			ID:    "4",
			Score: 3,
		},
	}

	idsWithScore2 := []redblocks.IDWithScore{
		{
			ID:    "1",
			Score: -100,
		},
		{
			ID:    "2",
			Score: -100,
		},
		{
			ID:    "3",
			Score: -100,
		},
	}

	err := redisStore.Save(ctx, key1, idsWithScore1, 100*time.Second)
	if err != nil {
		t.Error(err)
	}

	err = redisStore.Save(ctx, key2, idsWithScore2, 100*time.Second)
	if err != nil {
		t.Error(err)
	}

	err = redisStore.Subtraction(ctx, key, time.Second*100, key1, key2)
	if err != nil {
		t.Error(err)
	}
	result, err := redisStore.GetIDsWithScore(ctx, key, 0, -1, redblocks.Asc)
	if err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(result, []redblocks.IDWithScore{{ID: "4", Score: 3}}); diff != "" {
		t.Errorf(diff)
	}
}
