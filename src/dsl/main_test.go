package main

import (
	"dsl/workflow/dag_workflow"
	"go.uber.org/cadence/testsuite"
	"testing"
	"github.com/stretchr/testify/suite"
	"encoding/json"
)

type UnitTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *UnitTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
}

func (s *UnitTestSuite) AfterTest(suiteName, testName string) {
	s.env.AssertExpectations(s.T())
}

func (s *UnitTestSuite) Test_SimpleDagExecution() {
	// Setup a simple representation of DagWorkflowData
	dagData := dag_workflow.DagWorkflowData{
		Meta: dag_workflow.DagWfMeta{
			Name:         "test_wf",
			MaxDuration:  "1m",
			MaxStartTime: "1m",
		},
		Nodes: []*dag_workflow.DagStatement{
			{
				Name: "node1",
				Sequence: &dag_workflow.DagSequence{
					Elements: []*dag_workflow.DagStatement{
						{
							Activity: &dag_workflow.DagActivityInvocation{
								Name: "main.sampleActivity1",
								Arguments: []string{"arg1"},
								Result: "res1",
								MaximumAttempts: 1,
							},
						},
					},
				},
			},
		},
		Root: []string{"node1"},
		Variables: map[string]string{
			"arg1": "test-data",
		},
	}

	payload, _ := json.Marshal(dagData)
	wf := &dag_workflow.DagWorkflowData{}
	json.Unmarshal(payload, wf)

	s.env.ExecuteWorkflow(dag_workflow.DagWorkflow, *wf)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func TestUnitTestSuite(t *testing.T) {
	suite.Run(t, new(UnitTestSuite))
}
