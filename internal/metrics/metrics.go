package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the node's Prometheus collectors in its own registry, so nothing is global.
type Metrics struct {
	reg      *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// New creates the registry and registers every collector.
func New() *Metrics {
	m := &Metrics{
		reg: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dynakv_requests_total",
			Help: "Requests handled, by operation and gRPC result.",
		}, []string{"op", "result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "dynakv_request_duration_seconds",
			Help:    "Request latency, by operation.",
			Buckets: prometheus.DefBuckets,
		}, []string{"op"}),
	}
	m.reg.MustRegister(m.requests, m.duration)
	return m
}

// ObserveRequest records one finished request.
func (m *Metrics) ObserveRequest(op, result string, d time.Duration) {
	m.requests.WithLabelValues(op, result).Inc()
	m.duration.WithLabelValues(op).Observe(d.Seconds())
}

// Handler serves the metrics in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
