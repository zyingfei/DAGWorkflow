package helper

import (
	"context"
	"fmt"
	"io/ioutil"

	s "go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/worker"
	"go.uber.org/zap"

	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/client"
	"gopkg.in/yaml.v2"
	"go.uber.org/cadence/workflow"
	"time"
	"github.com/pborman/uuid"
	"encoding/json"
)

type (
	// DslHelper class for workflow dsl helper.
	DslHelper struct {
		Service workflowserviceclient.Interface
		Scope   tally.Scope
		Logger  *zap.Logger
		Config  Configuration
		Builder *WorkflowClientBuilder
	}

	// Configuration for running dsls.
	Configuration struct {
		DomainName      string   `yaml:"domain"`
		ServiceName     string   `yaml:"service"`
		HostNameAndPort string   `yaml:"host"`
		Tasklist        []string `yaml:"tasklist"`
		ActivityWorker  bool     `yaml:"activity_worker"`
		WorkflowWorker  bool     `yaml:"workflow_worker"`
	}
)

var domainCreated bool

// SetupServiceConfig setup the config for the dsls code run
func (h *DslHelper) SetupServiceConfig(configFile string) {
	if h.Service != nil {
		return
	}

	// Initialize developer config for running dsls
	configData, err := ioutil.ReadFile(configFile)
	if err != nil {
		panic(fmt.Sprintf("Failed to log config file: %v, Error: %v", configFile, err))
	}

	if err := yaml.Unmarshal(configData, &h.Config); err != nil {
		panic(fmt.Sprintf("Error initializing configuration: %v", err))
	}

	// Initialize logger for running dsls
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	logger.Info("Logger created.")
	h.Logger = logger
	h.Scope = tally.NoopScope
	h.Builder = NewBuilder(logger).
		SetHostPort(h.Config.HostNameAndPort).
		SetDomain(h.Config.DomainName).
		SetMetricsScope(h.Scope)
	service, err := h.Builder.BuildServiceClient()
	if err != nil {
		panic(err)
	}
	h.Service = service

	if domainCreated {
		return
	}
	domainClient, _ := h.Builder.BuildCadenceDomainClient()
	description := "domain for cadence dsls code"
	var retention int32 = 3
	request := &s.RegisterDomainRequest{
		Name:                                   &h.Config.DomainName,
		Description:                            &description,
		WorkflowExecutionRetentionPeriodInDays: &retention}

	err = domainClient.Register(context.Background(), request)
	if err != nil {
		if _, ok := err.(*s.DomainAlreadyExistsError); !ok {
			panic(err)
		}
		logger.Info("Domain already registered.", zap.String("Domain", h.Config.DomainName))
	} else {
		logger.Info("Domain succeesfully registered.", zap.String("Domain", h.Config.DomainName))
	}
	domainCreated = true
}

// StartWorkflow starts a workflow
func (h *DslHelper) StartWorkflow(options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) {
	workflowClient, err := h.Builder.BuildCadenceClient()
	if err != nil {
		h.Logger.Error("Failed to build cadence client.", zap.Error(err))
		panic(err)
	}

	we, err := workflowClient.StartWorkflow(context.Background(), options, workflow, args...)
	if err != nil {
		h.Logger.Error("Failed to create workflow", zap.Error(err))
		panic("Failed to create workflow.")

	} else {
		h.Logger.Info("Started Workflow", zap.String("WorkflowID", we.ID), zap.String("RunID", we.RunID))
	}
}

// Reset starts a workflow
func (h *DslHelper) ResetWorkflow(runId string, eventId *int64, workflowId string, workflow interface{}) string {

	ctx, _ := context.WithTimeout(context.Background(), 1*time.Minute)
	requestId := uuid.New()
	rsp, err := h.Service.ResetWorkflowExecution(ctx, &s.ResetWorkflowExecutionRequest{
		Domain: &h.Config.DomainName,
		WorkflowExecution: &s.WorkflowExecution{
			WorkflowId: &workflowId,
			RunId: &runId,
		},
		DecisionFinishEventId: eventId,
		RequestId: &requestId,

	},
	)

	if err != nil {
		h.Logger.Error("Failed to create workflow", zap.Error(err))
		panic("Failed to create workflow.")

	} else {
		h.Logger.Info("Reset Workflow", zap.String("WorkflowID", workflowId),
			zap.String("RunID", *rsp.RunId))

		return *rsp.RunId
	}
}


// StartWorkers starts workflow worker and activity worker based on configured options.
func (h *DslHelper) StartWorkers(domainName string, taskList []string, options worker.Options) {

	//create workers based on task
	for _, t := range taskList {
		worker := worker.New(h.Service, domainName, t, options)
		err := worker.Start()
		if err != nil {
			h.Logger.Error("Failed to start workers.", zap.Error(err))
			panic("Failed to start workers")
		}
	}
}

func (h *DslHelper) CompleteActivity(workflowId, runId, activityId string) {
	workflowClient, err := h.Builder.BuildCadenceClient()
	if err != nil {
		h.Logger.Error("Failed to build cadence client.", zap.Error(err))
		panic(err)
	}

	err = workflowClient.CompleteActivityByID(context.Background(), h.Config.DomainName,
		workflowId, runId, activityId, "Resume Activity", nil)

	if err != nil {
		panic(err)
	}

}
func (h *DslHelper) ReplayWorkflow(id string, runId string) error {

	ctx, _ := context.WithTimeout(context.Background(), 1*time.Minute)
	return worker.ReplayWorkflowExecution(ctx, h.Service, h.Logger, h.Config.DomainName, workflow.Execution{
		ID: id,
		RunID: runId,
	})

}
func (h *DslHelper) SignalExternalWorkflow(workflowId string, runId string, signalName string, payload []byte) {

	ctx, _ := context.WithTimeout(context.Background(), 1*time.Minute)

	requestId := uuid.New()

	//fixme: workaround string conversion
	payloadString := string(payload)
	b, _ := json.Marshal(payloadString)

	err := h.Service.SignalWorkflowExecution(ctx, &s.SignalWorkflowExecutionRequest{
		Domain: &h.Config.DomainName,
		WorkflowExecution: &s.WorkflowExecution{
			WorkflowId: &workflowId,
			RunId: &runId,
		},
		SignalName: &signalName,
		Input: b,
		RequestId: &requestId,
	}, )

	if err != nil {
		panic(err)
	}

}
