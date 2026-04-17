package workflow

import (
	"go.uber.org/cadence/client"
	"time"
)

type GenericWorkflow interface {
	GetWfOptions() client.StartWorkflowOptions
	GetMaxDuration() time.Duration
}
