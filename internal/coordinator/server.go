package coordinator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	pb "distributed-pow/gen/pow"
	"distributed-pow/internal/pow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type WorkerInfo struct {
	ID                string    `json:"worker_id"`
	Address           string    `json:"address"`
	LastHeartbeat     time.Time `json:"last_heartbeat"`
	Unavailable       bool      `json:"-"`
	Alive             bool      `json:"alive"`
	CurrentAssignment string    `json:"current_assignment"`
}

type workRange struct{ start, end uint64 }

type Job struct {
	mu          sync.Mutex
	ID          string     `json:"job_id"`
	Data        string     `json:"data"`
	Difficulty  int        `json:"difficulty"`
	Status      string     `json:"status"`
	Checked     uint64     `json:"checked"`
	Nonce       uint64     `json:"nonce,omitempty"`
	Hash        string     `json:"hash,omitempty"`
	Started     time.Time  `json:"started_at"`
	Finished    *time.Time `json:"finished_at,omitempty"`
	ctx         context.Context
	cancel      context.CancelFunc
	next        uint64
	retry       []workRange
	active      map[string]workRange
	launched    map[string]bool
	subscribers map[chan []byte]struct{}
	lastEvent   time.Time
}

func (j *Job) snapshot() map[string]any {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.snapshotLocked()
}
func (j *Job) snapshotLocked() map[string]any {
	elapsed := time.Since(j.Started)
	if j.Finished != nil {
		elapsed = j.Finished.Sub(j.Started)
	}
	result := map[string]any{
		"job_id": j.ID, "data": j.Data, "difficulty": j.Difficulty,
		"status": j.Status, "checked": j.Checked, "elapsed_seconds": elapsed.Seconds(),
		"started_at": j.Started.Format(time.RFC3339Nano),
	}
	if j.Status == "FOUND" {
		result["nonce"] = j.Nonce
		result["hash"] = j.Hash
	}
	return result
}
func (j *Job) publishLocked(event string) {
	payload, _ := json.Marshal(j.snapshotLocked())
	msg := []byte("event: " + event + "\ndata: " + string(payload) + "\n\n")
	for ch := range j.subscribers {
		select {
		case ch <- msg:
		default:
		}
	}
}
func (j *Job) finish(status string, nonce uint64, hash string) bool {
	j.mu.Lock()
	if j.Status != "RUNNING" {
		j.mu.Unlock()
		return false
	}
	j.Status = status
	j.Nonce = nonce
	j.Hash = hash
	t := time.Now()
	j.Finished = &t
	j.publishLocked("final")
	j.mu.Unlock()
	j.cancel()
	return true
}

type Server struct {
	pb.UnimplementedCoordinatorServiceServer
	mu               sync.RWMutex
	workers          map[string]*WorkerInfo
	jobs             map[string]*Job
	rangeSize        uint64
	heartbeatTimeout time.Duration
}

func NewServer() *Server { return NewConfiguredServer(1000000, 10*time.Second) }
func NewConfiguredServer(rangeSize uint64, heartbeatTimeout time.Duration) *Server {
	if rangeSize == 0 {
		rangeSize = 1000000
	}
	if heartbeatTimeout <= 0 {
		heartbeatTimeout = 10 * time.Second
	}
	return &Server{workers: make(map[string]*WorkerInfo), jobs: make(map[string]*Job), rangeSize: rangeSize, heartbeatTimeout: heartbeatTimeout}
}
func (s *Server) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	if req.GetWorkerId() == "" || req.GetAddress() == "" {
		return &pb.RegisterWorkerResponse{Success: false, Message: "worker ID and address required"}, nil
	}
	s.mu.Lock()
	s.workers[req.GetWorkerId()] = &WorkerInfo{ID: req.GetWorkerId(), Address: req.GetAddress(), LastHeartbeat: time.Now()}
	s.mu.Unlock()
	log.Printf("REGISTERED worker=%s address=%s", req.GetWorkerId(), req.GetAddress())
	return &pb.RegisterWorkerResponse{Success: true, Message: "registered"}, nil
}
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.workers[req.GetWorkerId()]
	if !ok {
		return &pb.HeartbeatResponse{Acknowledged: false}, nil
	}
	// A failed Search RPC quarantines this registration. A restarted worker
	// must register again; heartbeats alone must not revive a dead endpoint.
	if w.Unavailable {
		return &pb.HeartbeatResponse{Acknowledged: false}, nil
	}
	w.LastHeartbeat = time.Now()
	return &pb.HeartbeatResponse{Acknowledged: true}, nil
}
func (s *Server) aliveWorkers() []WorkerInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	out := make([]WorkerInfo, 0, len(s.workers))
	for _, w := range s.workers {
		if !w.Unavailable && now.Sub(w.LastHeartbeat) <= s.heartbeatTimeout {
			out = append(out, *w)
		}
	}
	return out
}
func (s *Server) isAlive(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w, ok := s.workers[id]
	return ok && !w.Unavailable && time.Since(w.LastHeartbeat) <= s.heartbeatTimeout
}
func (s *Server) workerAddress(id string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if w, ok := s.workers[id]; ok {
		return w.Address
	}
	return ""
}
func (s *Server) ListWorkers() []WorkerInfo {
	s.mu.RLock()
	out := make([]WorkerInfo, 0, len(s.workers))
	for _, w := range s.workers {
		out = append(out, *w)
	}
	s.mu.RUnlock()
	s.mu.RLock()
	jobs := make([]*Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}
	s.mu.RUnlock()
	for i := range out {
		out[i].Alive = !out[i].Unavailable && time.Since(out[i].LastHeartbeat) <= s.heartbeatTimeout
		active := make([]string, 0)
		for _, j := range jobs {
			j.mu.Lock()
			r, ok := j.active[out[i].ID]
			if ok {
				active = append(active, fmt.Sprintf("%s [%d,%d)", j.ID, r.start, r.end))
			}
			j.mu.Unlock()
		}
		sort.Strings(active)
		out[i].CurrentAssignment = strings.Join(active, ", ")
	}
	sort.Slice(out, func(i, k int) bool { return out[i].ID < out[k].ID })
	return out
}

func (s *Server) newJob(data string, difficulty int) *Job {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	j := &Job{ID: hex.EncodeToString(b), Data: data, Difficulty: difficulty, Status: "RUNNING", Started: time.Now(), ctx: ctx, cancel: cancel, active: make(map[string]workRange), launched: make(map[string]bool), subscribers: make(map[chan []byte]struct{})}
	s.mu.Lock()
	s.jobs[j.ID] = j
	s.mu.Unlock()
	go s.schedule(j)
	return j
}
func (s *Server) schedule(j *Job) {
	ticker := time.NewTicker(350 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-j.ctx.Done():
			return
		case <-ticker.C:
		}
		for _, w := range s.aliveWorkers() {
			j.mu.Lock()
			if j.Status != "RUNNING" {
				j.mu.Unlock()
				return
			}
			if j.launched[w.ID] {
				j.mu.Unlock()
				continue
			}
			j.launched[w.ID] = true
			j.mu.Unlock()
			go s.workerLoop(j, w.ID)
		}
	}
}
func (s *Server) nextRange(j *Job, id string) (workRange, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.Status != "RUNNING" {
		return workRange{}, false
	}
	var r workRange
	if len(j.retry) > 0 {
		r = j.retry[0]
		j.retry = j.retry[1:]
		log.Printf("RANGE REASSIGNED job=%s worker=%s range=[%d,%d)", j.ID, id, r.start, r.end)
	} else {
		if j.next > ^uint64(0)-s.rangeSize {
			return workRange{}, false
		}
		r = workRange{j.next, j.next + s.rangeSize}
		j.next = r.end
	}
	j.active[id] = r
	return r, true
}
func (s *Server) completeRange(j *Job, id string, r workRange, failed bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	delete(j.active, id)
	if failed && j.Status == "RUNNING" {
		j.retry = append(j.retry, r)
		log.Printf("RANGE REQUEUED job=%s failed-worker=%s range=[%d,%d)", j.ID, id, r.start, r.end)
	}
}

// quarantineWorker prevents repeated scheduling against a known failed endpoint.
// The next RegisterWorker call (e.g. after process restart) clears this state.
func (s *Server) quarantineWorker(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w, ok := s.workers[id]; ok && !w.Unavailable {
		w.Unavailable = true
		log.Printf("WORKER OFFLINE worker=%s: Search RPC failed; waiting for re-registration", id)
	}
}

func (s *Server) workerLoop(j *Job, id string) {
	defer func() { j.mu.Lock(); delete(j.launched, id); j.mu.Unlock() }()
	for {
		if j.ctx.Err() != nil || !s.isAlive(id) {
			return
		}
		r, ok := s.nextRange(j, id)
		if !ok {
			return
		}
		err := s.searchRange(j, id, r)
		if err != nil && j.ctx.Err() == nil {
			// Mark this endpoint unavailable before the scheduler can select it again.
			s.quarantineWorker(id)
		}
		s.completeRange(j, id, r, err != nil)
		if err != nil {
			if j.ctx.Err() == nil {
				log.Printf("SEARCH FAILED job=%s worker=%s: %v", j.ID, id, err)
			}
			return
		}
	}
}
func (s *Server) searchRange(j *Job, id string, r workRange) error {
	addr := s.workerAddress(id)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	// Deadline protects against a stalled worker. Failed ranges are retried.
	ctx, cancel := context.WithTimeout(j.ctx, 30*time.Second)
	defer cancel()
	stream, err := pb.NewWorkerServiceClient(conn).Search(ctx, &pb.SearchRequest{JobId: j.ID, Data: j.Data, Difficulty: int32(j.Difficulty), StartNonce: r.start, EndNonce: r.end})
	if err != nil {
		return err
	}
	var previous uint64
	for {
		update, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return errors.New("search stream ended without completed result")
		}
		if err != nil {
			return err
		}
		if update.GetJobId() != j.ID || update.GetWorkerId() != id {
			return errors.New("mismatched worker/job progress")
		}
		count := update.GetChecked()
		if count < previous || count > r.end-r.start {
			return errors.New("invalid progress count")
		}
		delta := count - previous
		previous = count
		j.mu.Lock()
		j.Checked += delta
		if time.Since(j.lastEvent) > 400*time.Millisecond {
			j.lastEvent = time.Now()
			j.publishLocked("progress")
		}
		j.mu.Unlock()
		if update.GetFound() {
			nonce := update.GetNonce()
			hash := update.GetHash()
			if nonce < r.start || nonce >= r.end || pow.Hash(j.Data, nonce) != hash || !pow.Valid(hash, j.Difficulty) {
				return errors.New("worker returned invalid solution")
			}
			if j.finish("FOUND", nonce, hash) {
				log.Printf("FOUND job=%s worker=%s nonce=%d hash=%s", j.ID, id, nonce, hash)
			}
			return nil
		}
		if update.GetCompleted() {
			if count != r.end-r.start {
				return errors.New("incomplete range reported as complete")
			}
			return nil
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
func (s *Server) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs", s.jobsHandler)
	mux.HandleFunc("/jobs/", s.jobHandler)
	mux.HandleFunc("/workers", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(405)
			return
		}
		writeJSON(w, 200, s.ListWorkers())
	})
	return mux
}
func (s *Server) jobsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		var req struct {
			Data       string `json:"data"`
			Difficulty int    `json:"difficulty"`
		}
		if err := dec.Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": "invalid JSON: " + err.Error()})
			return
		}
		var extra any
		if err := dec.Decode(&extra); err != io.EOF {
			writeJSON(w, 400, map[string]string{"error": "body must contain one JSON object"})
			return
		}
		if strings.TrimSpace(req.Data) == "" || req.Difficulty < 1 || req.Difficulty > 8 {
			writeJSON(w, 400, map[string]string{"error": "data required and difficulty must be 1..8"})
			return
		}
		job := s.newJob(req.Data, req.Difficulty)
		log.Printf("JOB SUBMITTED id=%s difficulty=%d", job.ID, job.Difficulty)
		writeJSON(w, http.StatusAccepted, map[string]string{"job_id": job.ID})
	case http.MethodGet:
		s.mu.RLock()
		jobs := make([]*Job, 0, len(s.jobs))
		for _, j := range s.jobs {
			jobs = append(jobs, j)
		}
		s.mu.RUnlock()
		out := make([]map[string]any, 0, len(jobs))
		for _, j := range jobs {
			out = append(out, j.snapshot())
		}
		sort.Slice(out, func(i, k int) bool { return out[i]["job_id"].(string) < out[k]["job_id"].(string) })
		writeJSON(w, 200, out)
	default:
		w.WriteHeader(405)
	}
}
func (s *Server) jobHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/jobs/")
	if path == "" {
		http.NotFound(w, r)
		return
	}
	isEvents := strings.HasSuffix(path, "/events")
	if isEvents {
		path = strings.TrimSuffix(path, "/events")
	}
	if strings.Contains(path, "/") {
		http.NotFound(w, r)
		return
	}
	s.mu.RLock()
	j, ok := s.jobs[path]
	s.mu.RUnlock()
	if !ok {
		writeJSON(w, 404, map[string]string{"error": "job not found"})
		return
	}
	if isEvents {
		if r.Method != http.MethodGet {
			w.WriteHeader(405)
			return
		}
		s.streamEvents(w, r, j)
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, 200, j.snapshot())
	case http.MethodDelete:
		if !j.finish("CANCELLED", 0, "") {
			writeJSON(w, 409, map[string]string{"error": "job already finished"})
			return
		}
		log.Printf("CANCELLED job=%s", j.ID)
		writeJSON(w, 200, j.snapshot())
	default:
		w.WriteHeader(405)
	}
}
func (s *Server) streamEvents(w http.ResponseWriter, r *http.Request, j *Job) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	ch := make(chan []byte, 32)
	j.mu.Lock()
	done := j.Status != "RUNNING"
	initial, _ := json.Marshal(j.snapshotLocked())
	if !done {
		j.subscribers[ch] = struct{}{}
	}
	j.mu.Unlock()
	defer func() { j.mu.Lock(); delete(j.subscribers, ch); j.mu.Unlock() }()
	event := "progress"
	if done {
		event = "final"
	}
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, initial)
	flusher.Flush()
	if done {
		return
	}
	ping := time.NewTicker(10 * time.Second)
	defer ping.Stop()
	for {
		select {
		case msg := <-ch:
			if _, err := w.Write(msg); err != nil {
				return
			}
			flusher.Flush()
			if strings.HasPrefix(string(msg), "event: final") {
				return
			}
		case <-j.ctx.Done():
			snap, _ := json.Marshal(j.snapshot())
			fmt.Fprintf(w, "event: final\ndata: %s\n\n", snap)
			flusher.Flush()
			return
		case <-r.Context().Done():
			return
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}
