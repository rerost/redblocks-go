package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/google/go-cmp/cmp"
	"github.com/rerost/redblocks-go/pkg/store"
)

func TestRediStoreGetIDs(t *testing.T) {
	redisStore := store.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})
	key := "TestRediStoreGetIDs"
	ctx := context.Background()
	err := redisStore.Save(ctx, key, []store.IDsWithScore{
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

	ids, err := redisStore.GetIDs(ctx, key, 0, -1)
	if err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(ids, []store.ID{"1", "2"}); diff != "" {
		t.Errorf(diff)
	}
}

func TestRediStoreGetIDsWithScore(t *testing.T) {
	redisStore := store.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})
	key := "TestRediStoreGetIDsWithScore"
	ctx := context.Background()

	idsWithScore := []store.IDsWithScore{
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

	result, err := redisStore.GetIDsWithScore(ctx, key, 0, -1)
	if err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(result, idsWithScore); diff != "" {
		t.Errorf(diff)
	}
}

func TestRedisStoreExists(t *testing.T) {
	redisStore := store.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})
	key := "TestRedisStoreExists"
	ctx := context.Background()

	idsWithScore := []store.IDsWithScore{
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

func TestRedisStoreInterstore(t *testing.T) {
	redisStore := store.NewRedisStore(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:6379") },
	})
	key := "TestRedisStoreInterstore"
	key1 := "TestRedisStoreInterstore1"
	key2 := "TestRedisStoreInterstore2"
	ctx := context.Background()

	idsWithScore1 := []store.IDsWithScore{
		{
			ID:    "1",
			Score: 1,
		},
		{
			ID:    "2",
			Score: 2,
		},
	}

	idsWithScore2 := []store.IDsWithScore{
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

	err = redisStore.Interstore(ctx, key, key1, key2)
	if err != nil {
		t.Error(err)
	}
	result, err := redisStore.GetIDsWithScore(ctx, key, 0, -1)
	if err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(result, []store.IDsWithScore{{ID: "1", Score: 2}, {ID: "2", Score: 4}}); diff != "" {
		t.Errorf(diff)
	}
}
