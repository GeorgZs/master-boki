package common

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	eventInitOnce sync.Once
	eventMu       sync.Mutex
	eventPath     string
	runID         string
	eventsEnabled bool
	allowedEvents map[string]struct{}
	workerFuncID  uint32
	workerClient  uint32
)

var reservedEventFields = map[string]struct{}{
	"schema_version": {},
	"run_id":         {},
	"system":         {},
	"ts_ms":          {},
	"event_type":     {},
	"ok":             {},
}

const defaultAllowedEventTypes = "invoke_end,state_read,state_write,txn_retry,txn_conflict,worker_start,worker_ready,scale_up_start,scale_up_end,scale_down_start,scale_down_end,placement_change"

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}

func parseAllowedEvents(raw string) map[string]struct{} {
	if strings.TrimSpace(raw) == "" {
		raw = defaultAllowedEventTypes
	}
	out := make(map[string]struct{})
	for _, token := range strings.Split(raw, ",") {
		if eventType := strings.TrimSpace(token); eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	return out
}

func initEventConfig() {
	eventPath = firstNonEmpty(
		os.Getenv("BOKI_EVENTS_JSONL_PATH"),
		os.Getenv("BOKI_EVENTS_JSONL"),
	)
	runID = firstNonEmpty(
		os.Getenv("BOKI_RUN_ID"),
		os.Getenv("RUN_ID"),
	)
	allowedEvents = parseAllowedEvents(os.Getenv("BOKI_EVENT_TYPES"))
	eventsEnabled = eventPath != "" && runID != ""
}

func SetWorkerIdentity(funcID uint16, clientID uint16) {
	atomic.StoreUint32(&workerFuncID, uint32(funcID))
	atomic.StoreUint32(&workerClient, uint32(clientID))
}

func EmitBokiEvent(eventType string, ok bool, fields map[string]interface{}) {
	eventInitOnce.Do(initEventConfig)
	if !eventsEnabled {
		return
	}
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return
	}
	if _, exists := allowedEvents[eventType]; !exists {
		return
	}

	record := map[string]interface{}{
		"schema_version": "1.0",
		"run_id":         runID,
		"system":         "boki",
		"ts_ms":          time.Now().UnixMilli(),
		"event_type":     eventType,
		"ok":             ok,
	}

	if fields != nil {
		for k, v := range fields {
			if _, reserved := reservedEventFields[k]; reserved {
				continue
			}
			record[k] = v
		}
	}

	if _, exists := record["function_id"]; !exists {
		if id := atomic.LoadUint32(&workerFuncID); id != 0 {
			record["function_id"] = strconv.FormatUint(uint64(id), 10)
		}
	}
	if _, exists := record["component_id"]; !exists {
		funcID := atomic.LoadUint32(&workerFuncID)
		clientID := atomic.LoadUint32(&workerClient)
		if funcID != 0 || clientID != 0 {
			record["component_id"] = strconv.FormatUint(uint64(funcID), 10) + ":" + strconv.FormatUint(uint64(clientID), 10)
		}
	}
	if _, exists := record["attributes"]; !exists {
		record["attributes"] = map[string]interface{}{}
	}

	line, err := json.Marshal(record)
	if err != nil {
		return
	}

	eventMu.Lock()
	defer eventMu.Unlock()

	f, err := os.OpenFile(eventPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(line, '\n'))
}
