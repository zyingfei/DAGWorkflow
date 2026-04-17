package main

import (
	"flag"
	"fmt"
	"io/ioutil"

	"go.uber.org/cadence/worker"
	"gopkg.in/yaml.v2"
	"dsl/helper"
	"reflect"
	"go.uber.org/cadence/workflow"
	"dsl/activityimpl"
	"dsl/workflow/dag_workflow"
	"dsl/workflow/simple_workflow"
	workflow2 "dsl/workflow"
	"go.uber.org/cadence/activity"
	"time"
	"math/rand"
)

var WorkflowNewFuncMap = map[string]interface{}{
	"SimpleDslWorkflow": simple_workflow.NewWorkflow,
	"DagWorkflow":       dag_workflow.NewDagWorkflow,
}

//registry of all applications and activities
var (
	ApplicationNames    = []string{dag_workflow.ApplicationName, simple_workflow.ApplicationName}
	ApplicationVersions = []string{dag_workflow.ApplicationVersion, simple_workflow.ApplicationVersion}

	WorkflowExecTypeMap = map[string]interface{}{
		"SimpleDslWorkflow": simple_workflow.SimpleDSLWorkflow,
		"DagWorkflow":       dag_workflow.DagWorkflow,
	}

	//internal activities has code dependencies, do not remove by user
	InternalActivities = map[string]interface{}{
		"signalActivity":         signalActivity,
		"pauseActivity":          pauseActivity,
		"dsl/activityimpl.Pause": activityimpl.Pause,
	}

	//user defined activities
	Activities = []interface{}{
		sampleActivity1,
		sampleActivity2,
		sampleActivity3,
		sampleActivity4,
		sampleActivity5,
		sampleVersionActivity,
		dynamicActivity,
		dynamicOnFalseConditionActivity,
		flipCoin,
		breakOnMaxNRevisitActivity,
		activityimpl.MonitorJob,
		activityimpl.RetryJob,
	}
)

// This needs to be done as part of a bootstrap step when the process starts.
// The workers are supposed to be long running.
func startWorkers(h *helper.DslHelper, applicationName string, ApplicationVersion string, config helper.Configuration) {
	// Configure worker options.
	workerOptions := worker.Options{
		MetricsScope:          h.Scope,
		Logger:                h.Logger,
		DisableActivityWorker: !config.ActivityWorker,
		DisableWorkflowWorker: !config.WorkflowWorker,
		NonDeterministicWorkflowPolicy: 1,
	}

	taskListWithGroupName := append(config.Tasklist, applicationName+"_"+ApplicationVersion)
	h.StartWorkers(h.Config.DomainName, taskListWithGroupName, workerOptions)
}

func startWorkflow(h *helper.DslHelper, wf workflow2.GenericWorkflow, wfType interface{}) {

	wst := reflect.ValueOf(wf).Elem().Interface()
	h.StartWorkflow(wf.GetWfOptions(), wfType, wst)

}


func resubmitWorkflow(h *helper.DslHelper, eventId *int64, runId string, workflowId string, dslConfig string, wfType interface{}) {

	var updatedPayload []byte
	if dslConfig != "" {
		var err error
		updatedPayload, err = ioutil.ReadFile(dslConfig)
		if err != nil {
			panic(fmt.Sprintf("failed to load dsl config file %v", err))
		}
	}

	newRunId := h.ResetWorkflow(runId, eventId, workflowId, wfType)


	h.SignalExternalWorkflow(workflowId, newRunId, dag_workflow.ReloadSignalName, updatedPayload)

}

// This is registration process where you register all your workflows
// and activity function handlers.
func init() {

	//register workflow
	for _, wf := range WorkflowExecTypeMap {
		workflow.Register(wf)
	}

	//register internal activities
	for _, act := range InternalActivities {
		activity.Register(act)
	}

	//register activities
	for _, act := range Activities {
		activity.Register(act)
	}
}

func main() {

	seed := time.Now().Unix();

	rand.Seed(seed)

	var mode, dslConfig, wfType, workflowId, runId, activityId, configFile string
	var verbose bool
	var eventId int64
	flag.StringVar(&mode, "m", "trigger", "Mode is worker, trigger, complete, pause, update or local.")
	flag.StringVar(&dslConfig, "dslConfig", "", "dslConfig specify the yaml file for the dsl workflow.")
	flag.StringVar(&wfType, "wfType", "DagWorkflow", "workflow type to parse the file")
	flag.BoolVar(&verbose, "v", false, "verbose")
	flag.StringVar(&workflowId, "w", "", "workflow id")
	flag.StringVar(&runId, "r", "", "run id")
	flag.StringVar(&activityId, "t", "", "activity id")
	flag.StringVar(&configFile, "config", "config/development.yaml", "config file path")
	flag.Int64Var(&eventId, "eventId", -1, "event id")
	flag.Parse()

	worker.EnableVerboseLogging(verbose)

	var h helper.DslHelper

	switch mode {
	case "complete":
		if workflowId == "" || activityId == "" {
			panic("workflowId or activityId not provided")
		}
		h.SetupServiceConfig(configFile)

		h.CompleteActivity(workflowId, runId, activityId)
	case "pause":
		if workflowId == "" {
			panic("workflow id not provided")
		}

		h.SetupServiceConfig(configFile)
		startUpdateDAGWorkflow("resources/workflow_dag_pause.yaml", workflowId, runId, h)

	case "update":
		if workflowId == "" {
			panic("workflow id not provided")
		}

		h.SetupServiceConfig(configFile)
		startUpdateDAGWorkflow(dslConfig, workflowId, runId, h)

	case "local":
		data, err := ioutil.ReadFile(dslConfig)
		if err != nil {
			panic(fmt.Sprintf("failed to load dsl config file %v", err))
		}

		wf := WorkflowNewFuncMap[wfType].(func() workflow2.GenericWorkflow)();
		if err := yaml.Unmarshal(data, wf); err != nil {
			panic(fmt.Sprintf("failed to unmarshal dsl config %v", err))
		}

		exec := new(LocalExecution)
		status, err, res := exec.ExecuteWorkflow(WorkflowExecTypeMap[wfType], wf)

		fmt.Printf("Workflow execution result is %v %+v and error (if exists) %+v\n", status, err, res)

	case "worker":
		h.SetupServiceConfig(configFile)

		//start worker based on registered keys
		for i := 0; i < len(ApplicationNames); i++ {
			startWorkers(&h, ApplicationNames[i], ApplicationVersions[i], h.Config)
		}

		// The workers are supposed to be long running process that should not exit.
		// Use select{} to block indefinitely for samples, you can quit by CMD+C.
		select {}
	case "replay":
		h.SetupServiceConfig(configFile)

		err := h.ReplayWorkflow(workflowId, runId)

		if err != nil {
			panic(err)
		}
	case "resubmit":
		h.SetupServiceConfig(configFile)

		resubmitWorkflow(&h, &eventId, runId, workflowId, dslConfig, WorkflowExecTypeMap[wfType])

	case "trigger":
		h.SetupServiceConfig(configFile)
		data, err := ioutil.ReadFile(dslConfig)
		if err != nil {
			panic(fmt.Sprintf("failed to load dsl config file %v", err))
		}
		workflow := WorkflowNewFuncMap[wfType].(func() workflow2.GenericWorkflow)();
		if err := yaml.Unmarshal(data, workflow); err != nil {
			panic(fmt.Sprintf("failed to unmarshal dsl config %v", err))
		}
		startWorkflow(&h, workflow, WorkflowExecTypeMap[wfType])
	default:
		panic("mode not supported.")
	}
}
func startUpdateDAGWorkflow(dslConfig string, workflowId string, runId string, h helper.DslHelper) {
	updatedPayload, err := ioutil.ReadFile(dslConfig)
	if err != nil {
		panic(fmt.Sprintf("failed to load dsl config file %v", err))
	}
	signalDag, err := ioutil.ReadFile("resources/workflow_dag_signal.yaml")
	if err != nil {
		panic(fmt.Sprintf("failed to load dsl config file %v", err))
	}
	workflow := WorkflowNewFuncMap["DagWorkflow"].(func() workflow2.GenericWorkflow)()
	if err := yaml.Unmarshal(signalDag, workflow); err != nil {
		panic(fmt.Sprintf("failed to unmarshal dag config %v", err))
	}
	//- workflowId
	//- runId
	//- updatedPayload
	workflow.(*dag_workflow.DagWorkflowData).Variables = make(map[string]string)
	workflow.(*dag_workflow.DagWorkflowData).Variables["workflowId"] = workflowId
	workflow.(*dag_workflow.DagWorkflowData).Variables["runId"] = runId
	workflow.(*dag_workflow.DagWorkflowData).Variables["updatedPayload"] = string(updatedPayload)
	startWorkflow(&h, workflow, WorkflowExecTypeMap["DagWorkflow"])
}
