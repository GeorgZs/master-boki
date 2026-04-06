package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	faas "cs.utexas.edu/zjia/faas"
	"cs.utexas.edu/zjia/faas/types"
)

// ClientFactory invokes statefulBench via InvokeFunc.
type ClientFactory struct{}

func (f *ClientFactory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	return &ClientHandler{env: env}, nil
}

func (f *ClientFactory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	return nil, fmt.Errorf("gRPC not supported")
}

// ClientHandler is the benchmark driver function.
// When called, it invokes statefulBench N times and returns timing results.
type ClientHandler struct {
	env types.Environment
}

func (h *ClientHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	numRequests, _ := strconv.Atoi(os.Getenv("NUM_REQUESTS"))
	if numRequests <= 0 {
		numRequests = 5
	}
	stateSizeKB, _ := strconv.Atoi(os.Getenv("STATE_SIZE_KB"))
	if stateSizeKB <= 0 {
		stateSizeKB = 1
	}

	payload := fmt.Sprintf(`{"state_key":"bench:state","state_size_kb":%d,"ops":1}`, stateSizeKB)

	type Result struct {
		RequestID string  `json:"request_id"`
		BeginTs   float64 `json:"begin"`
		EndTs     float64 `json:"end"`
		E2EUs     int64   `json:"e2e_us"`
	}

	results := make([]Result, 0, numRequests)
	var totalUs int64

	for i := 0; i < numRequests; i++ {
		start := time.Now()
		output, err := h.env.InvokeFunc(ctx, "statefulBench", []byte(payload))
		elapsed := time.Since(start).Microseconds()

		if err != nil {
			return nil, fmt.Errorf("invocation %d failed: %w", i, err)
		}

		var parsed map[string]interface{}
		json.Unmarshal(output, &parsed)

		rid, _ := parsed["request_id"].(string)
		begin, _ := parsed["begin"].(float64)
		end, _ := parsed["end"].(float64)

		results = append(results, Result{
			RequestID: rid,
			BeginTs:   begin,
			EndTs:     end,
			E2EUs:     elapsed,
		})
		totalUs += elapsed
	}

	summary := map[string]interface{}{
		"num_requests":    numRequests,
		"state_size_kb":   stateSizeKB,
		"total_time_us":   totalUs,
		"avg_e2e_us":      totalUs / int64(numRequests),
		"results":         results,
	}

	return json.Marshal(summary)
}

func main() {
	faas.Serve(&ClientFactory{})
}
