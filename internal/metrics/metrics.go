package metrics

import "github.com/prometheus/client_golang/prometheus"

var JobsSubmitted = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "jobs_submitted_total",
		Help: "Total number of submitted jobs.",
	},
)

var JobsCompleted = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "jobs_completed_total",
		Help: "Total number of completed jobs.",
	},
)

var JobsFailed = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "jobs_failed_total",
		Help: "Total number of failed jobs.",
	},
)

func Register() {
	prometheus.MustRegister(JobsSubmitted)
	prometheus.MustRegister(JobsCompleted)
	prometheus.MustRegister(JobsFailed)
}
