package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"job-platform/internal/metrics"
	"log"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type JobPublisher interface {
	Publish(context.Context, string) error
}
type JobCache interface {
	SetJob(context.Context, Job) error
	GetJob(context.Context, string) (Job, error)
	Close() error
}

type Handler struct {
	repo      *Repository
	publisher JobPublisher
	cache     JobCache
}

func NewHandler(repo *Repository, publisher JobPublisher, cache JobCache) *Handler {
	return &Handler{repo: repo, publisher: publisher, cache: cache}
}
func validateCreateRequest(req CreateJobRequest) error {
	if req.Type == "" || req.Payload == "" {
		return errors.New("type and payload are required")
	}

	return nil
}

func (h *Handler) Create(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		http.Error(w, "Idempotency-Key header is required", http.StatusBadRequest)
		return
	}

	var req CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if err := validateCreateRequest(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	job := Job{
		ID:      uuid.NewString(),
		Type:    req.Type,
		Payload: req.Payload,
		Status:  "pending",
	}

	savedJob, created, err := h.repo.CreateWithIdempotency(
		r.Context(),
		job,
		idempotencyKey,
	)
	if err != nil {
		log.Println("failed to create idempotent job:", err)
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return
	}

	if !created {
		writeJSON(w, http.StatusAccepted, CreateJobResponse{
			ID:      savedJob.ID,
			Message: "job already accepted",
			Type:    savedJob.Type,
			Status:  savedJob.Status,
		})
		return
	}

	job = savedJob
	metrics.JobsSubmitted.Inc()

	if err := h.publisher.Publish(r.Context(), job.ID); err != nil {
		log.Println("failed to publish job:", err)
		http.Error(w, "failed to publish job", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusAccepted, CreateJobResponse{
		ID:      job.ID,
		Message: "job received",
		Type:    job.Type,
		Status:  job.Status,
	})
}

func (h *Handler) GetByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/jobs/")
	if id == "" {
		http.Error(w, "job ID is required", http.StatusBadRequest)
		return
	}

	job, err := h.cache.GetJob(r.Context(), id)
	if err == nil {
		writeJSON(w, http.StatusOK, job)
		return
	}

	log.Println("cache lookup failed, reading from PostgreSQL:", err)

	job, err = h.repo.GetByID(r.Context(), id)
	if errors.Is(err, pgx.ErrNoRows) {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	if err != nil {
		log.Println("failed to get job:", err)
		http.Error(w, "failed to get job", http.StatusInternalServerError)
		return
	}

	if err := h.cache.SetJob(r.Context(), job); err != nil {
		log.Println("failed to cache job:", err)
	}

	writeJSON(w, http.StatusOK, job)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Println("failed to encode JSON:", err)
	}
}
