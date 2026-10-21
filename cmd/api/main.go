package main

import (
	"context"
	"job-platform/internal/cache"
	"job-platform/internal/database"
	"job-platform/internal/jobs"
	"job-platform/internal/messaging"
	"job-platform/internal/metrics"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	ctx := context.Background()

	pool, err := database.Connect(ctx)
	if err != nil {
		log.Fatal("database connection failed:", err)
	}
	defer pool.Close()

	metrics.Register()
	http.Handle("/metrics", promhttp.Handler())

	publisher := messaging.NewPublisher("localhost:9092", "jobs")
	defer publisher.Close()

	cache := cache.NewRedisCache("localhost:6379")
	defer cache.Close()

	repo := jobs.NewRepository(pool)

	handler := jobs.NewHandler(repo, publisher, cache)

	http.HandleFunc("/jobs", handler.Create)
	http.HandleFunc("/jobs/", handler.GetByID)

	log.Println("API listening on http://localhost:8080")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
