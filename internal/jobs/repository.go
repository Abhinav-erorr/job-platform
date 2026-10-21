package jobs

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (repo *Repository) Create(ctx context.Context, job Job) error {
	_, err := repo.pool.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, status)
		VALUES ($1, $2, $3, $4)
	`, job.ID, job.Type, job.Payload, job.Status)

	return err
}

func (repo *Repository) GetByID(ctx context.Context, id string) (Job, error) {
	var job Job

	err := repo.pool.QueryRow(ctx, `
		SELECT id, type, payload, status
		FROM jobs
		WHERE id = $1
	`, id).Scan(
		&job.ID,
		&job.Type,
		&job.Payload,
		&job.Status,
	)

	if err == pgx.ErrNoRows {
		return Job{}, pgx.ErrNoRows
	}

	return job, err
}
func (repo *Repository) CreateWithIdempotency(
	ctx context.Context,
	job Job,
	key string,
) (Job, bool, error) {
	tx, err := repo.pool.Begin(ctx)
	if err != nil {
		return Job{}, false, err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, status)
		VALUES ($1, $2, $3, $4)
	`, job.ID, job.Type, job.Payload, job.Status)
	if err != nil {
		return Job{}, false, err
	}

	result, err := tx.Exec(ctx, `
		INSERT INTO job_idempotency (idempotency_key, job_id)
		VALUES ($1, $2)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, key, job.ID)
	if err != nil {
		return Job{}, false, err
	}

	if result.RowsAffected() == 0 {
		var existingID string

		err = tx.QueryRow(ctx, `
			SELECT job_id
			FROM job_idempotency
			WHERE idempotency_key = $1
		`, key).Scan(&existingID)
		if err != nil {
			return Job{}, false, err
		}

		if err := tx.Rollback(ctx); err != nil {
			return Job{}, false, err
		}

		existingJob, err := repo.GetByID(ctx, existingID)
		return existingJob, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Job{}, false, err
	}

	return job, true, nil
}
