package simple_workflow

import (
	"fmt"
	"github.com/pborman/uuid"
	"go.uber.org/cadence/client"
	"sync"

	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
	"strconv"
	"time"
	workflow2 "dsl/workflow"
)

// ApplicationName is the task list for this sample
const ApplicationName = "SimpleRunner"
const ApplicationVersion = "0.0.1"

const ApplicationUID = ApplicationName + "_" + ApplicationVersion;

const DefaultMaxDuration = 60 * time.Minute;

const DefaultMaxStartTime = 5 * time.Minute;

type (
	// Workflow is the type used to express the workflow definition. Variables are a map of valuables. Variables can be
	// used as input to Activity.
	Workflow struct {
		Meta      WfMeta
		Variables map[string]string
		Root      Statement
		Result    Result
	}

	WfMeta struct {
		Name         string
		MaxDuration  string `yaml:"max_duration"`
		MaxStartTime string `yaml:"max_start_time"`
	}
	// Statement is the building block of dsl workflow. A Statement can be a simple ActivityInvocation or it
	// could be a Sequence or Parallel.
	Statement struct {
		Activity *ActivityInvocation
		Sequence *Sequence
		Parallel *Parallel
		//MaxDuration     string
	}

	// Sequence consist of a collection of Statements that runs in sequential.
	Sequence struct {
		Elements        []*Statement
		MaximumAttempts int64 `yaml:"maximum_attempts"`
		Loop            bool
	}

	// Parallel can be a collection of Statements that runs in parallel.
	Parallel struct {
		Branches []*Statement
	}

	// ActivityInvocation is used to express invoking an Activity. The Arguments defined expected arguments as input to
	// the Activity, the result specify the name of variable that it will store the result as which can then be used as
	// arguments to subsequent ActivityInvocation.
	ActivityInvocation struct {
		Name                 string
		Arguments            []string
		Result               string
		MaxDuration          string `yaml:"max_duration"`
		MaximumAttempts      int32  `yaml:"maximum_attempts"`
		Heartbeat            bool
		ResultBreakCondition string `yaml:"result_break_condition"`
	}

	executable interface {
		execute(ctx workflow.Context, bindings *sync.Map) error
	}

	Result struct {
		Info map[string]string
	}
)

//workflow interface impl
func (w *Workflow) GetMaxDuration() time.Duration {
	maxDuration, err := time.ParseDuration(w.Meta.MaxDuration);
	if err != nil {
		maxDuration = DefaultMaxDuration;
	}

	return maxDuration
}
func (w *Workflow) GetWfOptions() client.StartWorkflowOptions {

	maxStartTime, err := time.ParseDuration(w.Meta.MaxStartTime);
	if err != nil {
		maxStartTime = DefaultMaxStartTime;
	}

	workflowOptions := client.StartWorkflowOptions{
		ID:                              "dsl_" + w.Meta.Name + "_" + uuid.New(),
		TaskList:                        ApplicationUID,
		ExecutionStartToCloseTimeout:    w.GetMaxDuration(),
		DecisionTaskStartToCloseTimeout: maxStartTime,
	}

	return workflowOptions;
}

func NewWorkflow() workflow2.GenericWorkflow {
	return new(Workflow)
}

// SimpleDSLWorkflow workflow decider
func SimpleDSLWorkflow(ctx workflow.Context, dslWorkflow Workflow) (Result, error) {
	var result Result
	result.Info = map[string]string{}
	bindings := sync.Map{}
	for k, v := range dslWorkflow.Variables {
		bindings.Store(k, v)
	}
	//bind attempt count
	bindings.Store("attempt", "1")

	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		HeartbeatTimeout:       0,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	logger := workflow.GetLogger(ctx)

	err := dslWorkflow.Root.execute(ctx, &bindings)
	if err != nil {
		logger.Error("DSL Workflow failed.", zap.Error(err))
		return result, err
	}

	logger.Info("DSL Workflow completed.")
	for k, v := range dslWorkflow.Result.Info {
		value, _ := bindings.Load(v)
		result.Info[k] = value.(string)
	}
	return result, err
}

func (b *Statement) execute(ctx workflow.Context, bindings *sync.Map) error {
	if b.Parallel != nil {
		err := b.Parallel.execute(ctx, bindings)
		if err != nil {
			return err
		}
	}
	if b.Sequence != nil {
		err := b.Sequence.execute(ctx, bindings)
		if err != nil {
			return err
		}
	}
	if b.Activity != nil {
		err := b.Activity.execute(ctx, bindings)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a ActivityInvocation) execute(ctx workflow.Context, bindings *sync.Map) error {
	localCtx := ctx
	heartbeat := a.Heartbeat
	var heartbeatInterval time.Duration

	if heartbeat {
		heartbeatInterval = 20 * time.Second
	}

	//logic to retry defined in DSL
	if a.MaximumAttempts != 0 {

		duration, err := time.ParseDuration(a.MaxDuration);
		if err != nil {
			duration = DefaultMaxDuration;
		}

		localCtx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ScheduleToStartTimeout: duration,
			StartToCloseTimeout:    duration,
			HeartbeatTimeout:       heartbeatInterval,
			RetryPolicy: &workflow.RetryPolicy{
				MaximumAttempts:    a.MaximumAttempts,
				BackoffCoefficient: 2,
				InitialInterval:    1 * time.Second,
				ExpirationInterval: duration,
			},
		})

	}

	inputParam := makeInput(a.Arguments, bindings)
	var result string
	err := workflow.ExecuteActivity(localCtx, a.Name, inputParam).Get(localCtx, &result)
	if err != nil {
		return err
	}
	if a.Result != "" {
		bindings.Store(a.Result, result)
	}

	return nil
}

func (s Sequence) execute(ctx workflow.Context, bindings *sync.Map) error {
	var err error
	loopFlag := true
	for _, a := range s.Elements {
		err = a.execute(ctx, bindings)
		if err != nil {
			return err
		}
		activityResultStr, _ := bindings.Load(a.Activity.Result)
		if a.Activity != nil && a.Activity.ResultBreakCondition != "" && a.Activity.ResultBreakCondition == activityResultStr.(string) {
			loopFlag = false
		}
	}
	if s.MaximumAttempts == 0 {
		s.MaximumAttempts = 1
	}
	logger := workflow.GetLogger(ctx)
	attemptStr, _ := bindings.Load("attempt")
	logger.Info(fmt.Sprintf("%s/%d attempt of sequence execution", attemptStr, s.MaximumAttempts))

	if loopFlag {

		attemptCt, attemptErr := strconv.ParseInt(attemptStr.(string), 10, 64)
		if attemptErr != nil {
			return attemptErr
		}
		if attemptCt < s.MaximumAttempts {
			attemptCt++
			bindings.Store("attempt", strconv.FormatInt(attemptCt, 10))
			err = s.execute(ctx, bindings)
		}
	}
	return err
}

func (p Parallel) execute(ctx workflow.Context, bindings *sync.Map) error {
	//
	// You can use the context passed in to activity as a way to cancel the activity like standard GO way.
	// Cancelling a parent context will cancel all the derived contexts as well.
	//

	// In the parallel block, we want to execute all of them in parallel and wait for all of them.
	// if one activity fails then we want to cancel all the rest of them as well.
	childCtx, cancelHandler := workflow.WithCancel(ctx)
	selector := workflow.NewSelector(ctx)
	var activityErr error
	for _, s := range p.Branches {
		f := executeAsync(s, childCtx, bindings)
		selector.AddFuture(f, func(f workflow.Future) {
			err := f.Get(ctx, nil)
			if err != nil {
				// cancel all pending activities
				cancelHandler()
				activityErr = err
			}
		})
	}

	for i := 0; i < len(p.Branches); i++ {
		selector.Select(ctx) // this will wait for one branch
		if activityErr != nil {
			return activityErr
		}
	}

	return nil
}

func executeAsync(exe executable, ctx workflow.Context, bindings *sync.Map) workflow.Future {
	future, settable := workflow.NewFuture(ctx)
	workflow.Go(ctx, func(ctx workflow.Context) {
		err := exe.execute(ctx, bindings)

		settable.Set(nil, err)
	})

	return future
}

func makeInput(argNames []string, argsMap *sync.Map) []string {
	var args []string
	for _, arg := range argNames {
		storedValue, _ := argsMap.Load(arg)
		args = append(args, storedValue.(string))
	}

	return args
}
