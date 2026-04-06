package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	faas "cs.utexas.edu/zjia/faas"
	"cs.utexas.edu/zjia/faas/slib/statestore"
	"cs.utexas.edu/zjia/faas/types"
)

// Input is the JSON payload from SeBS / the HTTP gateway.
// Fields use json.Number to handle both int values (POST body)
// and string values (GET query string via qsAsInput).
type Input struct {
	StateKey    string      `json:"state_key"`
	StateSizeKB json.Number `json:"state_size_kb"`
	Ops         json.Number `json:"ops"`
	RequestID   string      `json:"request_id"`
}

func (in *Input) stateSizeKB() int {
	v, _ := in.StateSizeKB.Int64()
	if v < 1 {
		return 1
	}
	return int(v)
}

func (in *Input) ops() int {
	v, _ := in.Ops.Int64()
	if v < 1 {
		return 1
	}
	return int(v)
}

// Output matches the SeBS ExecutionResult format.
type Output struct {
	RequestID   string      `json:"request_id"`
	IsCold      bool        `json:"is_cold"`
	Begin       float64     `json:"begin"`
	End         float64     `json:"end"`
	Measurement Measurement `json:"measurement"`
}

// Measurement contains per-invocation timing data.
type Measurement struct {
	ComputeTimeUs   int64 `json:"compute_time_us"`
	StateReadLatUs  int64 `json:"state_read_lat_us"`
	StateWriteLatUs int64 `json:"state_write_lat_us"`
	StateSizeKB     int   `json:"state_size_kb"`
	StateOps        int   `json:"state_ops"`
	Accumulator     int   `json:"accumulator"`
}

// Factory implements types.FuncHandlerFactory.
type Factory struct{}

func (f *Factory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	return &Handler{env: env}, nil
}

func (f *Factory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	return nil, fmt.Errorf("gRPC not supported")
}

// Handler implements types.FuncHandler.
type Handler struct {
	env types.Environment
}

func (h *Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	var in Input
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, fmt.Errorf("failed to parse input: %w", err)
	}

	// Defaults
	stateSizeKB := in.stateSizeKB()
	ops := in.ops()
	if in.StateKey == "" {
		in.StateKey = "bench:state"
	}
	if in.RequestID == "" {
		in.RequestID = fmt.Sprintf("%016x", h.env.GenerateUniqueID())
	}

	begin := float64(time.Now().UnixNano()) / 1e9

	// Create a non-transactional statestore environment.
	slibEnv := statestore.CreateEnv(ctx, h.env)
	obj := slibEnv.Object(in.StateKey)

	// Generate a random string payload of the requested size.
	// The statestore uses JSON + Snappy compression internally,
	// so this exercises the full slib write path.
	payload := randomString(stateSizeKB * 1024)

	// --- State write (shared log append) ---
	t0 := time.Now()
	wr := obj.SetString("data", payload)
	if wr.Err != nil {
		return nil, fmt.Errorf("state write failed: %w", wr.Err)
	}
	stateWriteLatUs := time.Since(t0).Microseconds()

	// --- State read (shared log replay) ---
	// Sync to latest log tail to force a fresh read.
	if err := obj.Sync(); err != nil {
		return nil, fmt.Errorf("sync failed: %w", err)
	}
	t1 := time.Now()
	val, err := obj.Get("data")
	if err != nil {
		return nil, fmt.Errorf("state read failed: %w", err)
	}
	_ = val // We don't need the value, just the latency.
	stateReadLatUs := time.Since(t1).Microseconds()

	// --- Lightweight compute (same loop as Python benchmarks) ---
	t2 := time.Now()
	acc := 0
	limit := ops * 64
	if limit > 20000 {
		limit = 20000
	}
	for idx := 0; idx < limit; idx++ {
		acc = (acc + idx + stateSizeKB) % 1000003
	}
	computeTimeUs := time.Since(t2).Microseconds()

	end := float64(time.Now().UnixNano()) / 1e9

	out := Output{
		RequestID: in.RequestID,
		IsCold:    false,
		Begin:     begin,
		End:       end,
		Measurement: Measurement{
			ComputeTimeUs:   computeTimeUs,
			StateReadLatUs:  stateReadLatUs,
			StateWriteLatUs: stateWriteLatUs,
			StateSizeKB:     stateSizeKB,
			StateOps:        ops,
			Accumulator:     acc,
		},
	}

	return json.Marshal(out)
}

// randomString generates a random alphanumeric string of length n.
func randomString(n int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var b strings.Builder
	b.Grow(n)
	for i := 0; i < n; i++ {
		b.WriteByte(chars[rand.Intn(len(chars))])
	}
	return b.String()
}

func main() {
	faas.Serve(&Factory{})
}
