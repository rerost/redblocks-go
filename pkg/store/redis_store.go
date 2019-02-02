package store

import (
	"context"
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
		err := conn.Send("ZADD", key, idWithScore.ID, idWithScore.Score)
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

func (s redisStoreImp) GetIDs(ctx context.Context, key string, head int64, tail int64) ([]ID, error) {
	conn := s.pool.Get()

	IDs, err := redis.Strings(conn.Do("ZRANGE", key, head, tail))
	if err != nil {
		return []ID{}, fail.Wrap(err)
	}

	ids := make([]ID, len(IDs), len(IDs))
	for i, id := range IDs {
		ids[i] = ID(id)
	}
	return ids, nil
}

func (s redisStoreImp) GetIDsWithScore(ctx context.Context, key string, head int64, tail int64) ([]IDWithScore, error) {
	conn := s.pool.Get()

	results, err := redis.Strings(conn.Do("ZRANGE", key, head, tail, "WITHSCORES"))
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

func (s redisStoreImp) Interstore(ctx context.Context, dst string, keys ...string) error {
	conn := s.pool.Get()
	args := make([]interface{}, len(keys)+2, len(keys)+2)
	args[0] = dst
	args[1] = len(keys)
	for i, k := range keys {
		args[i+2] = k
	}

	_, err := conn.Do("ZINTERSTORE", args...)
	return fail.Wrap(err)
}

func (s redisStoreImp) Unionstore(ctx context.Context, dst string, keys ...string) error {
	conn := s.pool.Get()
	args := make([]interface{}, len(keys)+2, len(keys)+2)
	args[0] = dst
	args[1] = len(keys)
	for i, k := range keys {
		args[i+2] = k
	}

	_, err := conn.Do("ZUNIONSTORE", args...)
	return fail.Wrap(err)
}
