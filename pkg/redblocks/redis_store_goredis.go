package redblocks

import (
	"context"
	"fmt"
	"time"

	go_redis "github.com/go-redis/redis"
	"github.com/srvc/fail"
)

type RedisClientFunc func(context context.Context) *go_redis.Client

func NewGoredisStore(redisClientFunc RedisClientFunc) Store {
	return newGoredisStoreImp{
		redisClientFunc: redisClientFunc,
	}
}

type newGoredisStoreImp struct {
	redisClientFunc RedisClientFunc
}

func (s newGoredisStoreImp) Save(ctx context.Context, key string, idsWithScore []IDWithScore, expire time.Duration) error {
	redisClient := s.redisClientFunc(ctx)
	pipe := redisClient.Pipeline()

	for _, idWithScore := range idsWithScore {
		pipe.ZAdd(key, go_redis.Z{Member: string(idWithScore.ID), Score: idWithScore.Score})
	}

	pipe.Expire(key, expire)
	_, err := pipe.Exec()

	return fail.Wrap(err)
}

func (s newGoredisStoreImp) GetIDs(ctx context.Context, key string, head int64, tail int64) ([]ID, error) {
	redisClient := s.redisClientFunc(ctx)

	cmd := redisClient.ZRange(key, head, tail)
	if err := cmd.Err(); err != nil {
		return []ID{}, fail.Wrap(err)
	}
	IDs := cmd.Val()

	ids := make([]ID, len(IDs), len(IDs))
	for i, id := range IDs {
		ids[i] = ID(id)
	}
	return ids, nil
}

func (s newGoredisStoreImp) GetIDsWithScore(ctx context.Context, key string, head int64, tail int64) ([]IDWithScore, error) {
	redisClient := s.redisClientFunc(ctx)

	rangeCmd := redisClient.ZRangeWithScores(key, head, tail)
	if err := rangeCmd.Err(); err != nil {
		return []IDWithScore{}, fail.Wrap(err)
	}
	results := rangeCmd.Val()

	idsWithScore := make([]IDWithScore, len(results), len(results))
	for i, result := range results {
		idsWithScore[i] = IDWithScore{
			ID:    ID(result.Member.(string)),
			Score: result.Score,
		}
	}

	return idsWithScore, nil
}

func (s newGoredisStoreImp) Exists(ctx context.Context, key string) (bool, error) {
	redisClient := s.redisClientFunc(ctx)

	cmd := redisClient.Exists(key)
	if err := cmd.Err(); err != nil {
		return false, fail.Wrap(err)
	}

	return cmd.Val() == 1, nil
}

func (s newGoredisStoreImp) TTL(ctx context.Context, key string) (time.Duration, error) {
	redisClient := s.redisClientFunc(ctx)

	cmd := redisClient.TTL(key)

	if err := cmd.Err(); err != nil {
		return 0, fail.Wrap(err)
	}
	result := cmd.Val()
	if result < 0 {
		if result == -2*time.Second {
			return 0, fail.Wrap(fail.New("Not found"), fail.WithParam("key", key))
		}
		if result == -1*time.Second {
			return 0, fail.Wrap(fail.New("Not configured expire"), fail.WithParam("key", key))
		}
		panic(fail.Wrap(fail.New(fmt.Sprintf("Returned unexpected ttl: %v", result)), fail.WithParam("key", key), fail.WithParam("ttl", result)))
	}

	return result, nil
}

func (s newGoredisStoreImp) Interstore(ctx context.Context, dst string, expire time.Duration, weights []float64, aggregate Aggregate, keys ...string) error {
	redisClient := s.redisClientFunc(ctx)

	pipe := redisClient.Pipeline()
	zstore := go_redis.ZStore{
		Weights:   weights,
		Aggregate: aggregate.String(),
	}
	pipe.ZInterStore(dst, zstore, keys...)
	pipe.Expire(dst, expire)

	_, err := pipe.Exec()
	return fail.Wrap(err)
}

func (s newGoredisStoreImp) Unionstore(ctx context.Context, dst string, expire time.Duration, weights []float64, aggregate Aggregate, keys ...string) error {
	redisClient := s.redisClientFunc(ctx)

	pipe := redisClient.Pipeline()
	zstore := go_redis.ZStore{
		Weights:   weights,
		Aggregate: aggregate.String(),
	}
	pipe.ZUnionStore(dst, zstore, keys...)
	pipe.Expire(dst, expire)

	_, err := pipe.Exec()
	return fail.Wrap(err)
}

// WARING: This function is experimental.
// Because
// - Slow
// - set2's score needs to be much larger than set1' sscore
// - set2's score needs to be a negative value
func (s newGoredisStoreImp) Subtraction(ctx context.Context, dst string, expire time.Duration, key1 string, key2 string) error {
	redisClient := s.redisClientFunc(ctx)

	pipe := redisClient.TxPipeline()
	zstore := go_redis.ZStore{
		Weights:   []float64{1, 1},
		Aggregate: "SUM",
	}
	pipe.ZUnionStore(dst, zstore, key1, key2)
	pipe.ZRemRangeByScore(dst, "-inf", "(0")
	pipe.Expire(dst, expire)

	_, err := pipe.Exec()
	return fail.Wrap(err)
}

func (s newGoredisStoreImp) Count(ctx context.Context, key string) (int64, error) {
	redisClient := s.redisClientFunc(ctx)

	cmd := redisClient.ZCard(key)
	if err := cmd.Err(); err != nil {
		return 0, fail.Wrap(err)
	}
	return cmd.Val(), nil
}
