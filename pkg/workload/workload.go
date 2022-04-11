package workload

import "context"

// Workloader is the interface for running customized workload.
type Workloader interface {
	Name() string
	DBName() string
	InitThread(ctx context.Context) context.Context
	CleanupThread(ctx context.Context)
	Prepare(ctx context.Context) error
	Run(ctx context.Context) error
	Cleanup(ctx context.Context) error
}
