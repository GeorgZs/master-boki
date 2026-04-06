package main

import (
	"context"
	"fmt"
	"time"

	faas "cs.utexas.edu/zjia/faas"
	"cs.utexas.edu/zjia/faas/types"
)

type Factory struct{}

func (f *Factory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	return &Handler{}, nil
}

func (f *Factory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	return nil, fmt.Errorf("not supported")
}

type Handler struct{}

func (h *Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	now := float64(time.Now().UnixNano()) / 1e9
	return []byte(fmt.Sprintf(`{"echo":true,"ts":%f,"input_len":%d}`, now, len(input))), nil
}

func main() {
	faas.Serve(&Factory{})
}
