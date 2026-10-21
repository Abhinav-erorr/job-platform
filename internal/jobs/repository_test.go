package jobs

import (
	"context"
	"testing"
	"time"

	"job-platform/internal/database"

	"github.com/google/uuid"
)

func TestCreateWithIdempotency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := database.Connect(ctx)
	if err != nil {
		t.Skipf("PostgreSQL is unavailable: %v", err)
	}
	defer pool.Close()

	key := "test-" + uuid.NewString()
	defer pool.Exec(ctx,
		"DELETE FROM job_idempotency WHERE idempotency_key = $1",
		key,
	)

	repo := NewRepository(pool)

	firstJob := Job{
		ID:      uuid.NewString(),
		Type:    "email",
		Payload: "integration test",
		Status:  "pending",
	}

	createdJob, created, err := repo.CreateWithIdempotency(ctx, firstJob, key)
	if err != nil {
		t.Fatalf("first creation failed: %v", err)
	}
	if !created {
		t.Fatal("first request should create a job")
	}

	secondJob := Job{
		ID:      uuid.NewString(),
		Type:    "email",
		Payload: "integration test",
		Status:  "pending",
	}

	duplicateJob, created, err := repo.CreateWithIdempotency(ctx, secondJob, key)
	if err != nil {
		t.Fatalf("duplicate creation failed: %v", err)
	}
	if created {
		t.Fatal("duplicate request should not create a second job")
	}

	if duplicateJob.ID != createdJob.ID {
		t.Fatalf(
			"expected same job ID, got first=%s second=%s",
			createdJob.ID,
			duplicateJob.ID,
		)
	}
}
