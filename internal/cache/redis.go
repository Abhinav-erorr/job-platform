package cache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"job-platform/internal/jobs"
)

type RedisCache struct {
	client *redis.Client
}

func NewRedisCache(addr string) *RedisCache {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisCache{client: client}
}

func (r *RedisCache) SetJob(ctx context.Context, job jobs.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return r.client.Set(ctx, "job:"+job.ID, data, 5*time.Minute).Err()

}
func (c *RedisCache) GetJob(ctx context.Context, id string) (jobs.Job, error) {
	data, err := c.client.Get(ctx, "job:"+id).Bytes()
	if err != nil {
		return jobs.Job{}, err
	}

	var job jobs.Job
	if err := json.Unmarshal(data, &job); err != nil {
		return jobs.Job{}, err
	}

	return job, nil
}
func (c *RedisCache) Close() error {
	return c.client.Close()
}
