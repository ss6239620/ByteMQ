package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/sharvesh/bytemq/internal/app"
	"github.com/sharvesh/bytemq/internal/store"
)

type server struct {
	service *app.Service
}

type enqueueJobRequest struct {
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotencyKey"`
}

type jobResponse struct {
	ID             string          `json:"id"`
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	State          string          `json:"state"`
	Attempt        int             `json:"attempt"`
	IdempotencyKey string          `json:"idempotencyKey,omitempty"`
}

func NewHandler(service *app.Service) http.Handler {
	s := &server{service: service}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/jobs", s.handleJobs)
	mux.HandleFunc("/v1/jobs/", s.handleJob)
	return mux
}

func (s *server) handleJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var request enqueueJobRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request")
		return
	}

	job, err := s.service.EnqueueJob(r.Context(), app.EnqueueRequest{
		Queue:          request.Queue,
		Type:           request.Type,
		Payload:        request.Payload,
		IdempotencyKey: request.IdempotencyKey,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job")
		return
	}

	writeJSON(w, http.StatusCreated, toJobResponse(job))
}

func (s *server) handleJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/v1/jobs/")
	if strings.TrimSpace(id) == "" {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}

	job, err := s.service.GetJob(r.Context(), id)
	if errors.Is(err, store.ErrJobNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	writeJSON(w, http.StatusOK, toJobResponse(job))
}

func toJobResponse(job store.JobRecord) jobResponse {
	return jobResponse{
		ID:             job.ID,
		Queue:          job.Queue,
		Type:           job.Type,
		Payload:        job.Payload,
		State:          string(job.State),
		Attempt:        job.Attempt,
		IdempotencyKey: job.IdempotencyKey,
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
