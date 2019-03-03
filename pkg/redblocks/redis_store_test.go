package redblocks_test

import (
	"context"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/google/go-cmp/cmp"
	"github.com/rerost/redblocks-go/pkg/redblocks"
)

func TestRediStoreGetIDs(t *testing.T) {
	redisStore := redblocks.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})
	key := "TestRediStoreGetIDs"
	ctx := context.Background()
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

func TestRediStoreGetIDsWithScore(t *testing.T) {
	redisStore := redblocks.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})
	key := "TestRediStoreGetIDsWithScore"
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

func TestRedisStoreExists(t *testing.T) {
	redisStore := redblocks.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})
	key := "TestRedisStoreExists"
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

func TestRediStoreTTL(t *testing.T) {
	redisStore := redblocks.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})

	key := "TestRedisStoreTTL"

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
	if !(0 < ttl && ttl < cacheTime) {
		t.Errorf("want: 0 < ttl < cacheTime but ttl: %v", ttl)
	}

	emptyKey := key + ":" + "EMPTY"
	_, err = redisStore.TTL(ctx, emptyKey)
	if diff := cmp.Diff(err.Error(), "Not found"); diff != "" {
		t.Errorf(diff)
	}

	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		t.Error(err)
	}

	notExpireKey := key + ":" + "NOT_EXPIRE"
	_, err = conn.Do("SET", notExpireKey, notExpireKey)
	if err != nil {
		t.Error(err)
	}
	_, err = redisStore.TTL(ctx, notExpireKey)
	if diff := cmp.Diff(err.Error(), "Not configured expire"); diff != "" {
		t.Errorf(diff)
	}
	conn.Do("DEL", notExpireKey)
}

func TestRedisStoreInterstore(t *testing.T) {
	redisStore := redblocks.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})
	key := "TestRedisStoreInterstore"
	key1 := "TestRedisStoreInterstore1"
	key2 := "TestRedisStoreInterstore2"
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

func TestRedisStoreUnionstore(t *testing.T) {
	redisStore := redblocks.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})
	key := "TestRedisStoreUnionstore"
	key1 := "TestRedisStoreUnionstore1"
	key2 := "TestRedisStoreUnionstore2"
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

func TestRedisStoreSubtraction(t *testing.T) {
	redisStore := redblocks.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})
	key := "TestRedisStoreSubtraction"
	key1 := "TestRedisStoreSubtraction1"
	key2 := "TestRedisStoreSubtraction2"
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

func TestRedisStoreIDsWithOrder(t *testing.T) {
	redisStore := redblocks.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})

	key := "TestRedisStoreIDsWithOrder"
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
	ctx := context.Background()

	err := redisStore.Save(ctx, key, idsWithScore, time.Second*100)
	if err != nil {
		t.Error(err)
	}

	ids, err := redisStore.GetIDs(ctx, key, 0, -1, redblocks.Asc)
	if err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(ids, []redblocks.ID{"1", "2"}); diff != "" {
		t.Error(diff)
	}

	ids, err = redisStore.GetIDs(ctx, key, 0, -1, redblocks.Desc)
	if err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(ids, []redblocks.ID{"2", "1"}); diff != "" {
		t.Error(diff)
	}
}

func TestRedisStoreIDWithScoresWithOrder(t *testing.T) {
	redisStore := redblocks.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})

	key := "TestRedisStoreIDWithScoresWithOrder"
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

	revIdsWithScore := []redblocks.IDWithScore{
		{
			ID:    "2",
			Score: 2,
		},
		{
			ID:    "1",
			Score: 1,
		},
	}
	ctx := context.Background()

	err := redisStore.Save(ctx, key, idsWithScore, time.Second*100)
	if err != nil {
		t.Error(err)
	}

	result, err := redisStore.GetIDsWithScore(ctx, key, 0, -1, redblocks.Asc)
	if err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(result, idsWithScore); diff != "" {
		t.Error(diff)
	}

	result, err = redisStore.GetIDsWithScore(ctx, key, 0, -1, redblocks.Desc)
	if err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(result, revIdsWithScore); diff != "" {
		t.Error(diff)
	}
}
