module cs.utexas.edu/zjia/faas/benchmarks/stateful

go 1.14

require (
	cs.utexas.edu/zjia/faas v0.0.0
	cs.utexas.edu/zjia/faas/slib v0.0.0
)

replace (
	cs.utexas.edu/zjia/faas => ../../worker/golang
	cs.utexas.edu/zjia/faas/slib => ../../slib
)
