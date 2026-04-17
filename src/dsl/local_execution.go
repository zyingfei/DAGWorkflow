package main

import (
	"go.uber.org/cadence/testsuite"
	"time"
	"reflect"
	"dsl/workflow"
)

type LocalExecution struct {
	testsuite.WorkflowTestSuite
}

func (s *LocalExecution) ExecuteWorkflow(workflow interface{}, wf workflow.GenericWorkflow) (bool, error, interface{}) {

	env := s.NewTestWorkflowEnvironment()

	duration := wf.GetMaxDuration()

	env.SetTestTimeout(duration + 5*time.Second)
	env.SetWorkflowTimeout(duration)

	wst := reflect.ValueOf(wf).Elem().Interface()
	env.ExecuteWorkflow(workflow, wst)

	var result interface{}
	env.GetWorkflowResult(&result)

	return env.IsWorkflowCompleted(), env.GetWorkflowError(), result
}
