package main

import (
	"context"
	"fmt"
	"job-platform/internal/database"
	"job-platform/internal/messaging"
	"job-platform/internal/metrics"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/jackc/pgx/v5/pgxpool"
)

type JobPublisher interface {
	Publish(context.Context, string) error
}

func recoverRunningJobs(ctx context.Context, pool *pgxpool.Pool, publisher JobPublisher) error {
	rows, err := pool.Query(ctx, `
		UPDATE jobs
		SET status = CASE
				WHEN attempts >= max_attempts THEN 'failed'
				ELSE 'pending'
			END,
			updated_at = NOW()
		WHERE status = 'running'
		RETURNING id, status
	`)
	if err != nil {
		return err
	}

	var retryIDs []string

	for rows.Next() {
		var id, status string

		if err := rows.Scan(&id, &status); err != nil {
			rows.Close()
			return err
		}

		if status == "pending" {
			retryIDs = append(retryIDs, id)
		} else {
			log.Println("recovered job reached retry limit:", id)
		}
	}

	err = rows.Err()
	rows.Close()

	if err != nil {
		return err
	}

	for _, id := range retryIDs {
		if err := publisher.Publish(ctx, id); err != nil {
			log.Println("failed to publish recovered job:", id, err)
			continue
		}

		log.Println("recovered job and published retry:", id)
	}

	return nil
}

func claimJob(ctx context.Context, pool *pgxpool.Pool, jobID string) (string, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE jobs
		SET status = 'running', attempts = attempts + 1, updated_at = NOW()
		WHERE id = $1 AND status = 'pending'
	`, jobID)
	if err != nil {
		return "", err
	}

	if err := tx.Commit(ctx); err != nil {
		return "", err
	}

	if tag.RowsAffected() == 0 {
		return "", nil
	}

	return jobID, nil
}

func processJob(ctx context.Context, pool *pgxpool.Pool, id string) error {
	if jobType, err := getJobType(ctx, pool, id); err != nil {
		return err
	} else if jobType == "fail" {
		return fmt.Errorf("simulated processing failure")
	}

	log.Println("processing job:", id)

	time.Sleep(2 * time.Second)

	_, err := pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'completed', updated_at = NOW()
		WHERE id = $1
	`, id)

	return err
}
func getJobType(ctx context.Context, pool *pgxpool.Pool, id string) (string, error) {
	var jobType string

	err := pool.QueryRow(ctx,
		`SELECT type FROM jobs WHERE id = $1`,
		id,
	).Scan(&jobType)

	return jobType, err
}
func markFailed(ctx context.Context, pool *pgxpool.Pool, id string, errMessage string) (bool, error) {
	var status string

	err := pool.QueryRow(ctx, `
		UPDATE jobs
		SET status = CASE
				WHEN attempts >= max_attempts THEN 'failed'
				ELSE 'pending'
			END,
			last_error = $2,
			updated_at = NOW()
		WHERE id = $1
		RETURNING status
	`, id, errMessage).Scan(&status)

	return status == "pending", err
}
func main() {
	ctx := context.Background()

	pool, err := database.Connect(ctx)
	if err != nil {
		log.Fatal("database connection failed:", err)
	}
	defer pool.Close()

	publisher := messaging.NewPublisher("localhost:9092", "jobs")
	defer publisher.Close()

	if err := recoverRunningJobs(ctx, pool, publisher); err != nil {
		log.Fatal("failed to recover running jobs:", err)
	}

	log.Println("worker started")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "jobs",
		GroupID: "worker-group",
	})
	defer reader.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Println("failed to read message:", err)
			continue
		}
		jobid := string(msg.Value)
		log.Println("received job from Kafka:", jobid)

		claimedjob, err := claimJob(ctx, pool, jobid)
		if err != nil {
			log.Println("failed to claim job:", err)
			continue
		}

		if claimedjob == "" {
			log.Println("job was not pending or does not exist:", jobid)
		} else {
			if err := processJob(ctx, pool, claimedjob); err != nil {
				log.Println("failed to process job:", err)

				retry, markErr := markFailed(ctx, pool, claimedjob, err.Error())
				if markErr != nil {
					log.Println("failed to update failed job:", markErr)
					continue
				}

				if retry {
					if publishErr := publisher.Publish(ctx, claimedjob); publishErr != nil {
						log.Println("failed to publish retry:", publishErr)
					} else {
						log.Println("retry published:", claimedjob)
					}
				}
			}
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Println("failed to commit Kafka message:", err)
			continue
		}
		metrics.JobsCompleted.Inc()
	}
}
