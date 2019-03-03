package redblocks

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/srvc/fail"
)

func NewRedisStore(pool *redis.Pool) Store {
	return redisStoreImp{
		pool: pool,
	}
}

type redisStoreImp struct {
	pool *redis.Pool
}

func (s redisStoreImp) Save(ctx context.Context, key string, idsWithScore []IDWithScore, expire time.Duration) error {
	conn := s.pool.Get()

	for _, idWithScore := range idsWithScore {
		err := conn.Send("ZADD", key, idWithScore.Score, idWithScore.ID)
		if err != nil {
			return fail.Wrap(err)
		}
	}

	conn.Send("EXPIRE", key, expire.Seconds())

	if err := conn.Flush(); err != nil {
		return fail.Wrap(err)
	}

	return nil
}

func (s redisStoreImp) GetIDs(ctx context.Context, key string, head int64, tail int64, order Order) ([]ID, error) {
	conn := s.pool.Get()

	var cmd string
	switch order {
	case Asc:
		cmd = "ZRANGE"
	case Desc:
		cmd = "ZREVRANGE"
	default:
		panic(fmt.Sprintf("Undefined order passed: %v", order.String()))
	}

	IDs, err := redis.Strings(conn.Do(cmd, key, head, tail))
	if err != nil {
		return []ID{}, fail.Wrap(err)
	}

	ids := make([]ID, len(IDs), len(IDs))
	for i, id := range IDs {
		ids[i] = ID(id)
	}
	return ids, nil
}

func (s redisStoreImp) GetIDsWithScore(ctx context.Context, key string, head int64, tail int64, order Order) ([]IDWithScore, error) {
	conn := s.pool.Get()

	var cmd string
	switch order {
	case Asc:
		cmd = "ZRANGE"
	case Desc:
		cmd = "ZREVRANGE"
	default:
		panic(fmt.Sprintf("Undefined order passed: %v", order.String()))
	}

	results, err := redis.Strings(conn.Do(cmd, key, head, tail, "WITHSCORES"))
	if err != nil {
		return []IDWithScore{}, fail.Wrap(err)
	}

	idsWithScore := make([]IDWithScore, len(results)/2, len(results)/2)
	for i, result := range results {
		if i%2 == 0 {
			idsWithScore[i/2].ID = ID(result)
		} else {
			score, err := strconv.ParseFloat(result, 64)
			if err != nil {
				return idsWithScore, fail.Wrap(err)
			}
			idsWithScore[(i-1)/2].Score = score
		}
	}

	return idsWithScore, nil
}

func (s redisStoreImp) Exists(ctx context.Context, key string) (bool, error) {
	conn := s.pool.Get()

	result, err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		return false, fail.Wrap(err)
	}

	return result, nil
}

func (s redisStoreImp) TTL(ctx context.Context, key string) (time.Duration, error) {
	conn := s.pool.Get()

	result, err := redis.Int64(conn.Do("TTL", key))

	if err != nil {
		return 0, fail.Wrap(err)
	}

	// See https://redis.io/commands/TTL
	if result < 0 {
		if result == -2 {
			return 0, fail.Wrap(fail.New("Not found"), fail.WithParam("key", key))
		}
		if result == -1 {
			return 0, fail.Wrap(fail.New("Not configured expire"), fail.WithParam("key", key))
		}
		panic(fail.Wrap(fail.New("Returned unexpected ttl"), fail.WithParam("key", key), fail.WithParam("ttl", result)))
	}

	return time.Duration(result * int64(time.Millisecond)), nil
}

func (s redisStoreImp) Interstore(ctx context.Context, dst string, expire time.Duration, weights []float64, aggregate Aggregate, keys ...string) error {
	conn := s.pool.Get()
	args := []interface{}{}
	args = append(args, dst)
	args = append(args, len(keys))
	for _, k := range keys {
		args = append(args, k)
	}
	args = append(args, "WEIGHTS")
	for _, w := range weights {
		args = append(args, w)
	}
	args = append(args, "AGGREGATE")
	args = append(args, aggregate.String())

	conn.Send("ZINTERSTORE", args...)
	conn.Send("EXPIRE", dst, expire.Seconds())
	err := conn.Flush()
	return fail.Wrap(err)
}

func (s redisStoreImp) Unionstore(ctx context.Context, dst string, expire time.Duration, weights []float64, aggregate Aggregate, keys ...string) error {
	conn := s.pool.Get()
	args := []interface{}{}
	args = append(args, dst)
	args = append(args, len(keys))
	for _, k := range keys {
		args = append(args, k)
	}
	args = append(args, "WEIGHTS")
	for _, w := range weights {
		args = append(args, w)
	}
	args = append(args, "AGGREGATE")
	args = append(args, aggregate.String())

	conn.Send("ZUNIONSTORE", args...)
	conn.Send("EXPIRE", dst, expire.Seconds())
	err := conn.Flush()
	return fail.Wrap(err)
}

// WARING: This function is experimental.
// Because
// - Slow
// - set2's score needs to be much larger than set1' sscore
// - set2's score needs to be a negative value
func (s redisStoreImp) Subtraction(ctx context.Context, dst string, expire time.Duration, key1 string, key2 string) error {
	conn := s.pool.Get()

	conn.Send("MULTI")
	conn.Send("ZUNIONSTORE", dst, 2, key1, key2, "WEIGHTS", 1, 1, "AGGREGATE", "SUM")
	conn.Send("ZREMRANGEBYSCORE", dst, "-inf", "(0")
	conn.Send("EXPIRE", dst, expire.Seconds())
	_, err := conn.Do("EXEC")
	return fail.Wrap(err)
}

func (s redisStoreImp) Count(ctx context.Context, key string) (int64, error) {
	conn := s.pool.Get()
	count, err := redis.Int64(conn.Do("ZCARD", key))
	return count, fail.Wrap(err)
}
