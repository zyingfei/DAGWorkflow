package dag_workflow

import (
	"go.uber.org/cadence/workflow"
	"strings"
	"time"
	"go.uber.org/cadence/client"
	"github.com/pborman/uuid"
	"dsl/dag"
	"github.com/hashicorp/terraform/tfdiags"
	"log"
	workflow2 "dsl/workflow"
	"gopkg.in/yaml.v2"
	"encoding/json"
	"go.uber.org/zap"
	"fmt"
	"github.com/pkg/errors"
	"sync"
)

const ApplicationName = "DAGRunner"
const ApplicationVersion = "0.0.1"
const (
	NodeInfoKey = "NodeInfo"
	StopOnErr   = "StopOnErr"
	NodeNameKey = "NodeNameKey"
	DefaultScope = "root"
	ScopeKey = "ScopeKey"
	DefaultScopeSep = "."
)

const (
	ReloadSignalName       = "DAG_Reload"
	InternalTaskTypeSignal = "signal"
	InternalTaskTypePause  = "pause"
)

var (
	DAGUpdateErr = errors.New("Dag update requested")
	IncorrectDAGErr = errors.New("DAG root items not correct")
)

const ApplicationUID = ApplicationName + "_" + ApplicationVersion;

type (
	// Dag workflow defines a dynamic model to execute
	DagWorkflowData struct {
		Meta      DagWfMeta
		Root      []string
		OnExit    []string `yaml:"on_exit"`
		Variables map[string]string
		Result    map[string]string
		Nodes     []*DagStatement
	}

	DagWfMeta struct {
		Name         string
		MaxDuration  string `yaml:"max_duration"`
		MaxStartTime string `yaml:"max_start_time"`
		Version      string
	}

	NodeInfo struct {
		Name string
	}

	DagStatement struct {
		Name     string
		Activity *DagActivityInvocation
		Sequence *DagSequence
		Parallel *DagParallel

		//scope by default is global
		Scope    string
		//proto means the new scope node can be copied from there
		Proto    bool

		//MaxDuration     string
		MaximumAttempts int32 `yaml:"maximum_attempts"`
		Next            []string
		StopOnErr       bool   `yaml:"stop_on_err"`
		NextKey         string `yaml:"next_key"`
		PreSleep        string `yaml:"pre_sleep"`
		PostSleep       string `yaml:"post_sleep"`
		TaskList        string `yaml:"tasklist"`
		Revisit         bool   `yaml:"revisit"`

		//internal variables not for yaml loading
		_internalReload  bool
		_internalRoot bool
	}

	// Sequence consist of a collection of Statements that runs in sequential.
	DagSequence struct {
		Elements []*DagStatement
	}

	// Parallel can be a collection of Statements that runs in parallel.
	DagParallel struct {
		Branches []*DagStatement
	}

	ActivityInput struct {
		Args     []string
		NodeInfo string
		// Activity version is a only an error check / error detection mechanism by design
		// So if the activity check the version number, it should throw error on unexpected activity
		// And it is not expected to be used as version routing purpose
		// To implement version routing purpose, use tasklist to create different tasks to be handled
		// by different workers
		ActivityVersion workflow.Version
	}

	ActivityOutput struct {
		Results []string
	}

	// ActivityInvocation is used to express invoking an Activity. The Arguments defined expected arguments as input to
	// the Activity, the result specify the name of variable that it will store the result as which can then be used as
	// arguments to subsequent ActivityInvocation.
	DagActivityInvocation struct {
		Name             string
		Arguments        []string
		Result           string
		MaxDuration      string `yaml:"max_duration"`
		MaximumAttempts  int32  `yaml:"maximum_attempts"`
		Heartbeat        bool
		InternalTaskType string `yaml:"internal_task_type"`
		Count            int
		Pause            bool
		PauseOnFail      bool `yaml:"pause_on_fail"`
		// see ActivityInput.Version for usage
		CheckVersion workflow.Version `yaml:"check_version"`
		Results          []string
	}

	dagExecutable interface {
		//note: by default, we are passing additionalParams
		execute(ctx workflow.Context, cancelFunc workflow.CancelFunc, bindings map[string]string, nestLevel int, additionParams ...interface{}) error
	}
)

const DefaultMaxDuration = 1 * time.Hour;

const DefaultMaxStartTime = 1 * time.Minute;

//workflow interface impl
func (w *DagWorkflowData) GetMaxDuration() time.Duration {
	maxDuration, err := time.ParseDuration(w.Meta.MaxDuration);
	if err != nil {
		maxDuration = DefaultMaxDuration;
	}

	return maxDuration
}

func NewDagWorkflow() workflow2.GenericWorkflow {
	return new(DagWorkflowData)
}

func (w *DagWorkflowData) GetWfOptions() client.StartWorkflowOptions {

	maxStartTime, err := time.ParseDuration(w.Meta.MaxStartTime);
	if err != nil {
		maxStartTime = DefaultMaxStartTime;
	}

	workflowOptions := client.StartWorkflowOptions{
		ID:                              "dag_" + w.Meta.Name + "_" + uuid.New(),
		TaskList:                        ApplicationUID,
		ExecutionStartToCloseTimeout:    w.GetMaxDuration(),
		DecisionTaskStartToCloseTimeout: maxStartTime,
	}

	return workflowOptions;
}

type WalkReturn struct {
	NodeWalked bool
	Error tfdiags.Diagnostics
}

func DagWorkflow(ctx workflow.Context, dagWorkflow DagWorkflowData) ([]byte, error) {

	originalDagWorkflowPayload, err := yaml.Marshal(dagWorkflow)
	previousDagWorkflowPayload := originalDagWorkflowPayload

	if err != nil {
		return nil, err
	}

	graph, err := constructDag(dagWorkflow, nil, nil)

	if err != nil {
		return nil, err
	}

	//check dag version
	if dagWorkflow.Meta.Version != "" && dagWorkflow.Meta.Version != ApplicationVersion {
		return nil, errors.New("DAG Version not supported")
	}

	bindings := make(map[string]string)
	for k, v := range dagWorkflow.Variables {
		bindings[getScopedKey(DefaultScope, k)] = v
	}

	ao := workflow.ActivityOptions{
		WaitForCancellation: true,
		ScheduleToStartTimeout: 30 * 24 * time.Hour,
		StartToCloseTimeout:    30 * 24 * time.Hour,
		HeartbeatTimeout:       0,

	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	ctx, cancelFunc := workflow.WithCancel(ctx)

	var cancelOnce sync.Once
	cancelOnceFunc := func() {
		cancelOnce.Do(func() {
			log.Printf("Calling cancel context to stop DAG")
			cancelFunc()
		})

	}
	//logger := workflow.GetLogger(ctx)

	var lastVisitedNode dag.Vertex
	err = workflow.SetQueryHandler(ctx, "last_visited", func() (dag.Vertex, error) {
		return lastVisitedNode, nil
	})

	if err != nil {
		return nil, err
	}

	var visitNodesInOrder []dag.Vertex
	err = workflow.SetQueryHandler(ctx, "visited_nodes", func() ([]dag.Vertex, error) {
		return visitNodesInOrder, nil
	})

	if err != nil {
		return nil, err
	}

	//log.Printf("%+v", string(v))
	err = workflow.SetQueryHandler(ctx, "dag", func() ([]byte, error) {
		//for debugging only
		v, _ := yaml.Marshal(dagWorkflow)
		return v, nil
	})

	if err != nil {
		return nil, err
	}

	visitedNodes := make(map[dag.Vertex]bool)
	processingNodes := make(map[dag.Vertex]workflow.Future)

	walkFunc := func(ctx workflow.Context, g *dag.DagGraph, cancelFunc workflow.CancelFunc, v dag.Vertex,
		gracefulCancel *dag.BoolStruct, vn map[dag.Vertex]bool) (bool, tfdiags.Diagnostics) {

		var diag tfdiags.Diagnostics
		var err error
		ret := false

		skipWalkFunc := false
		if pNode, ok := processingNodes[v];ok {

			log.Printf("Waiting on processing/processed node %v", v)
			// the node is already processing, no reentry, wait till future returns
			// if the node is gracefully cancelled upon reload, we need to trigger the job in this run
			// otherwise, return the job result
			var walkRet WalkReturn
			err = pNode.Get(ctx, &walkRet)

			if err == nil {
				diag = walkRet.Error
				ret = walkRet.NodeWalked

				if diag.Err() != nil {

					log.Printf("Error: %s checking with error %s", diag.Err().Error(), DAGUpdateErr.Error())
					if diag.Err().Error() == DAGUpdateErr.Error() {
						//continue on walkFunc, as previous DAG was not run

					} else {
						skipWalkFunc = true
					}
				} else {
					skipWalkFunc = true
				}

			} else {

				diag.Append(err)

				workflow.GetLogger(ctx).Error("Error", zap.Error(err), zap.String("NodeName", v.(string)), zap.String("StopOnErr",
					fmt.Sprintf("%+v", ctx.Value(StopOnErr))))
			}

		}

		if !skipWalkFunc {

			log.Printf("WalkFunc: %v", v)

			//check for workflow stop before execution of current block
			selector := workflow.NewSelector(ctx)

			selector.AddReceive(ctx.Done(), func(c workflow.Channel, more bool) {
				//if context is done, cancel the workflow execution
				err = errors.New("workflow stopped")
			})

			selector.AddDefault(func() {})
			selector.Select(ctx)

			// On DAG update request, graceful cancel the future
			if err == nil && gracefulCancel.Value {
				err = DAGUpdateErr
			}

			if err == nil {

				visitNodesInOrder = append(visitNodesInOrder, v)

				//if non root, skip first execution
				if !g.NodeMap[v].(*DagStatement)._internalRoot {
					//return directly if non root
					log.Printf("Skip execute %s as it is not root", v)
					ret = false
				} else {

					var future workflow.Future
					var settable workflow.Settable
					isTransientNode := isTransientNode(g, v)
					if !isTransientNode {

						future, settable = workflow.NewFuture(ctx)
						//only add to processing if the node is not transient
						processingNodes[v] = future

						ret = true
					}

					log.Printf("Executing %s", v)
					err = g.NodeMap[v].(*DagStatement).execute(ctx, cancelFunc, bindings, 0, gracefulCancel)

					lastVisitedNode = v

					if !isTransientNode {
						//always set the future result back, after the task executed
						settable.Set(WalkReturn{ret, diag}, nil)
					}
				}
			}

			if err != nil {
				diag.Append(err)

				workflow.GetLogger(ctx).Error("Error", zap.Error(err), zap.String("NodeName", v.(string)),
					zap.String("StopOnErr", fmt.Sprintf("%+v", ctx.Value(StopOnErr))))
			}

		}

		log.Printf("Walk %v ret: %t", v, ret)

		return ret, diag

	}

	isReturn := false
	continueOnWait := false
	selector := workflow.NewSelector(ctx)
	reloadCh := workflow.GetSignalChannel(ctx, ReloadSignalName)

	selector.AddReceive(ctx.Done(), func(c workflow.Channel, more bool) {
		log.Printf("Context is canceled")

		isReturn = true
		continueOnWait = false
	})

	selector.AddReceive(reloadCh, func(c workflow.Channel, more bool) {

		isReturn = false
		var dagSerialized string

		c.Receive(ctx, &dagSerialized)

		if dagSerialized == "" {
			dagSerialized = string(previousDagWorkflowPayload)
		}

		log.Printf("reload DAG")

		graph, err = updateDag(dagSerialized, visitedNodes, bindings)
		if err != nil {
			log.Printf("DAG update fail: %v", err)
			isReturn = true
		} else {
			//on correct reload, set the dag workflow to current one
			previousDagWorkflowPayload = []byte(dagSerialized)
		}

		//wait is interrupted
		continueOnWait = false
	})

	selector.AddDefault(func() {
		isReturn = true
		continueOnWait = true
	})

	//execute the wf until no updates
	for {

		walker := &dag.Walker{Callback: walkFunc, VisitedNodes: visitedNodes}

		isReturn = false
		continueOnWait = false
		walker.Update(ctx, cancelOnceFunc, graph)
		var dgErr tfdiags.Diagnostics
		var lastVisitedNode []dag.Vertex
		var completed bool
		// Wait
		wait: if lastVisitedNode, dgErr, completed = walker.Wait(ctx); dgErr.Err() != nil {
			log.Printf("Walker wait err: %s", dgErr.Err())
		}

		log.Printf("node %s completes", lastVisitedNode)

		if !completed {

			for _, n := range lastVisitedNode {
				log.Printf("Adding visited %+v", n)
				visitedNodes[n] = true
			}

			selector.Select(ctx)

			if continueOnWait {
				goto wait
			}

		} else {
			log.Printf("current DAG all node walked")
			selector.Select(ctx)
		}

		if isReturn {

			if err != nil {
				dgErr.Append(err)
			}

			finalizeCtx, _ := workflow.NewDisconnectedContext(ctx)
			//wait till all blocks done
			ok := false
			for !ok {
				_, _, ok = walker.Wait(finalizeCtx);
			}

			//workflow.Sleep(finalizeCtx, 1 * time.Second)

			var finalResult []byte
			if dagWorkflow.Result != nil {
				resMap := make(map[string]string)
				for k, v := range dagWorkflow.Result {
					resMap[k] = bindings[getScopedKey(DefaultScope, v)]
				}
				finalResult, _ = json.Marshal(resMap)
			}

			log.Printf("DAG execution completes with result %v", string(finalResult))
			return finalResult, dgErr.Err()
		}
	}
}
func getScopedKey(scope string, key string) string {

	if strings.HasPrefix(key, DefaultScope) {
		return key
	}

	return scope + DefaultScopeSep + key
}

func normalizeScopedKey(scope string) string {
	if !strings.HasPrefix(scope, DefaultScope) {
		scope = DefaultScope + DefaultScopeSep + scope
	}

	return scope
}

//check if the job is transient, so it can be repeatly run after every updates
func isTransientNode(graph *dag.DagGraph, v dag.Vertex) bool {
	return graph.NodeMap[v].(*DagStatement).Activity != nil &&
		(graph.NodeMap[v].(*DagStatement).Activity.InternalTaskType == InternalTaskTypePause ||
			graph.NodeMap[v].(*DagStatement).Activity.InternalTaskType == InternalTaskTypeSignal)
}

//update dag with new serialized data
func updateDag(dagSerialized string, visitedNodes map[dag.Vertex]bool, bindings map[string]string) (*dag.DagGraph, error) {

	var workflowData DagWorkflowData

	if err := yaml.Unmarshal([]byte(dagSerialized), &workflowData); err != nil {
		log.Print(fmt.Sprintf("failed to unmarshal dag config %v, fallback original", err))
		return nil, err
	}

	return constructDag(workflowData, visitedNodes, bindings)
}

func constructDag(dagWorkflow DagWorkflowData, visitedNodes map[dag.Vertex]bool, bindings map[string]string) (*dag.DagGraph, error) {

	dagNodeMap := make(map[interface{}]interface{})
	revisitMap := make(map[interface{}]*dag.RevisitInfo)
	protoMap := make(map[string]string)

	var g dag.AcyclicGraph

	//init original nodes
	for _, node := range dagWorkflow.Nodes {

		//set default scope or normalize it
		if node.Scope == "" {
			node.Scope = DefaultScope
		} else {
			node.Scope = normalizeScopedKey(node.Scope)
		}

		scopedNodeName := getScopedKey(node.Scope, node.Name)
		//register node
		g.Add(scopedNodeName)

		//register node in map
		copyOfNodeStmt := *node
		copyOfNodeStmt.Name = scopedNodeName
		dagNodeMap[scopedNodeName] = &copyOfNodeStmt

		if node.Proto {
			protoMap[scopedNodeName] = scopedNodeName
		}

		//if the node can be revisited, add to revisit map
		revisitMap[scopedNodeName] = &dag.RevisitInfo{NodeName: scopedNodeName}
	}

	//tag root items
	for _, rootItem := range dagWorkflow.Root {
		scopedRootItem := getScopedKey(DefaultScope, rootItem)
		if _, ok := dagNodeMap[scopedRootItem]; ok {
			dagNodeMap[scopedRootItem].(*DagStatement)._internalRoot = true
		} else {
			return nil, IncorrectDAGErr
		}
	}

	//scan again to construct edges
	//as current working map is dagNodeMap, we just use names only in scanlist
	var scanList []*DagStatement
	for _, node := range dagWorkflow.Nodes {
		scanList = append(scanList, dagNodeMap[getScopedKey(node.Scope, node.Name)].(*DagStatement))
	}

	for i := 0; i < len(scanList); i++ {

		node := scanList[i]
		if node.NextKey != "" {
			if visitedNodes != nil && visitedNodes[node.Name] {
				bindingKey := replaceTemplateKey(node.NextKey, node.Name)
				b, ok := bindings[getScopedKey(node.Scope, bindingKey)]
				if ok {
					err := json.Unmarshal([]byte(b), &node.Next)
					if err != nil {
						fmt.Printf("Error dump: map %v, key: %s value: %s", bindings, getScopedKey(node.Scope, bindingKey), bindings[getScopedKey(node.Scope, bindingKey)])
						log.Printf("Unable to unmarshal %s", b)
						return nil, err
					}

					if node.Next == nil {
						node.Next = []string{}
					}
				}
			} else {
				node.Next = []string{}
			}
		}

		for _, nextNode := range node.Next {

			//get node scope from next definition
			//e.g. root.scope1.flipcoin, root.flipcoin
			//if there's no ., default to current scope
			scopeNodeName := getScopedKey(node.Scope, nextNode)

			var protoNode string
			if _, ok := dagNodeMap[scopeNodeName]; ok {
				protoNode = scopeNodeName
			} else {
				var err error
				protoNode, err = queryProtoNode(scopeNodeName, protoMap)
				if err != nil {
					log.Panic(err)
				}
			}

			if dagNodeMap[scopeNodeName] != nil && !dagNodeMap[scopeNodeName].(*DagStatement).Revisit {
				//on node revisit
				g.Connect(dag.BasicEdge(node.Name, scopeNodeName))
			} else {
				//create new nodes

				//set the node's DAG reload flag to true
				node._internalReload = true

				//only create new revisit node if parent node has been visited
				if visitedNodes[node.Name] {
					//for revisitable nodes, create a new node name
					revisitNode, ok := revisitMap[scopeNodeName]

					nextNodeWithOrdinal := scopeNodeName
					if ok {
						nextNodeWithOrdinal = revisitNode.GetNextNodeName()
					} else {
						revisitMap[scopeNodeName] = &dag.RevisitInfo{NodeName: scopeNodeName}
					}

					//add the node to DAG
					g.Add(nextNodeWithOrdinal)
					g.Connect(dag.BasicEdge(node.Name, nextNodeWithOrdinal))

					//add next node info to node map
					copyOfNodeStmt := *(dagNodeMap[protoNode].(*DagStatement))
					//set the node's DAG reload flag to false on default
					copyOfNodeStmt._internalReload = false
					copyOfNodeStmt.Name = nextNodeWithOrdinal
					dagNodeMap[nextNodeWithOrdinal] = &copyOfNodeStmt

					//append list to next scan list
					scanList = append(scanList, &copyOfNodeStmt)
				}
			}
		}
	}

	//set non root to false if it being at least one dest & origin is not root
	for _, node := range scanList {

		//dagNodeMap[node.Name] is the copy of current node's status
		//don't use node._internalRoot here!!!
		//3/17/2019: modified scanlist to current node pointer, thus we can use node._internal now
		if node._internalRoot {
			ans, _ := g.Ancestors(node.Name)
			log.Printf("%s has ancestors %v", node.Name, ans.List())
			for _, v := range ans.List() {
				dagNodeMap[v].(*DagStatement)._internalRoot = true
			}
		}
	}

	g.TransitiveReduction()
	if err := g.Validate(); err != nil {
		return nil, err
	}

	//return graph for exec
	return &dag.DagGraph{Graph: &g, NodeMap: dagNodeMap}, nil

}
func queryProtoNode(scopedNode string, protoNodeMap map[string]string) (string, error) {

	//split after root
	scopes := strings.Split(scopedNode, DefaultScopeSep)

	for len(scopes) >= 2 {
		if protoNode, ok := protoNodeMap[strings.Join(scopes, DefaultScopeSep)];ok {
			return protoNode, nil
		}

		//trim one level from right-1
		scopes = append(scopes[:len(scopes)-2], scopes[len(scopes)-1])
	}

	return "", errors.New(fmt.Sprintf("Proto node for %s not found!", scopedNode))

}

func replaceTemplateKey(result string, nodeNameKey string) string {
	bindingKey := result
	if strings.Contains(result, "$$node_id") {
		bindingKey = strings.Replace(result, "$$node_id", nodeNameKey, 1)
	}
	return bindingKey
}


// Execute statement, request graceful cancel to other stmt exec if the result is dynamic
func (b *DagStatement) execute(ctx workflow.Context, cancelFunc workflow.CancelFunc, bindings map[string]string, nestLevel int, additionParams ...interface{}) error {

	log.Printf("statement %s starts", b.Name)

	currCtx := ctx
	if b.Name != "" {
		info, _ := json.Marshal(NodeInfo{Name: b.Name})
		currCtx = workflow.WithValue(currCtx, NodeInfoKey, string(info))
		currCtx = workflow.WithValue(currCtx, ScopeKey, b.Scope)
		currCtx = workflow.WithValue(currCtx, NodeNameKey, b.Name)
	}

	//set stop on err if it's not set
	if ctx.Value(StopOnErr) == nil && b.StopOnErr {
		currCtx = workflow.WithValue(currCtx, StopOnErr, "true")
	}

	//set tasklist if it is not null
	if b.TaskList != "" {
		currCtx = workflow.WithTaskList(currCtx, b.TaskList)
	}

	// sleep before stmt
	if b.PreSleep != "" {
		d, err := time.ParseDuration(b.PreSleep)
		log.Printf("sleep for %v", d)
		if err == nil {
			err = workflow.Sleep(currCtx, d)
			if err != nil {
				log.Printf("sleep interrupted %v", err)
				return err
			}
		}
	}

	if b.Parallel != nil {
		err := b.Parallel.execute(currCtx, cancelFunc, bindings, nestLevel+1, additionParams)
		if err != nil {
			return err
		}
	}
	if b.Sequence != nil {
		err := b.Sequence.execute(currCtx, cancelFunc, bindings, nestLevel+1, additionParams)
		if err != nil {
			return err
		}
	}
	if b.Activity != nil {
		err := b.Activity.execute(currCtx, cancelFunc, bindings, nestLevel+1, additionParams)
		if err != nil {
			return err
		}
	}

	// sleep after stmt
	if b.PostSleep != "" {
		d, err := time.ParseDuration(b.PostSleep)
		if err == nil {
			err = workflow.Sleep(currCtx, d)
			if err != nil {
				log.Printf("sleep interrupted %v", err)
				return err
			}
		}
	}

	if nestLevel == 0 {
		//if current dag is dynamic or next keys contains revisitable nodes, it needs to be reloaded
		if b.NextKey != "" || b._internalReload {
			workflow.GetSignalChannel(ctx, ReloadSignalName).SendAsync("")
		}

		//Check for DAG reload, if so, stop all sequent activity execution
		reloadCh := workflow.GetSignalChannel(currCtx, ReloadSignalName)
		selector := workflow.NewSelector(currCtx)

		//if there's an update to the wf, flag current execution, leave the updated data in the channel
		selector.AddDefault(func() {})
		selector.AddReceive(reloadCh, func(c workflow.Channel, more bool) {
			//graceful cancel is set to true
			log.Printf("DAG reload signal handled")
			additionParams[0].(*dag.BoolStruct).Value = true
		})
		selector.Select(ctx)
	}

	activityName := ""
	if b.Activity != nil {
		activityName = b.Activity.Name
	}
	log.Printf("statement %s:%s executed", b.Name, activityName)

	return nil
}

func (p *DagParallel) execute(ctx workflow.Context, cancelFunc workflow.CancelFunc, bindings map[string]string, nestLevel int, additionParams ...interface{}) error {
	//
	// You can use the context passed in to activity as a way to cancel the activity like standard GO way.
	// Cancelling a parent context will cancel all the derived contexts as well.

	// In the parallel block, we want to execute all of them in parallel and wait for all of them.
	// if one activity fails then we want to cancel all the rest of them as well.
	childCtx, cancelHandler := workflow.WithCancel(ctx)
	selector := workflow.NewSelector(ctx)
	var activityErr error
	for _, s := range p.Branches {
		//for parallel jobs, each job take the original context
		f := executeAsync(s, childCtx, cancelFunc, bindings, nestLevel, additionParams)
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

func (a *DagActivityInvocation) execute(ctx workflow.Context, cancelFunc workflow.CancelFunc, bindings map[string]string, nestLevel int, additionParams ...interface{}) error {
	localCtx := ctx
	heartbeat := a.Heartbeat
	var heartbeatInterval time.Duration

	if heartbeat {
		heartbeatInterval = 20 * time.Second
	}

	// logic to retry defined in DAG
	if a.MaximumAttempts != 0 {

		duration, err := time.ParseDuration(a.MaxDuration);
		if err != nil {
			duration = DefaultMaxDuration;
		}

		localCtx = workflow.WithActivityOptions(localCtx, workflow.ActivityOptions{
			WaitForCancellation: true,
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

	version := workflow.DefaultVersion
	if a.CheckVersion != 0 {
		//we are not expected to use different version on same activity in workflow
		//if so, go with tasklist instead or call different activities
		version = workflow.GetVersion(ctx, a.Name, workflow.DefaultVersion, a.CheckVersion)
	}

	inputParam := makeInput(ctx, a.Arguments, bindings, version)
	var result *ActivityOutput
	var err error

	// check for task types and execute
	switch a.InternalTaskType {
	// internal types handling
	// internal types are special types of tasks needs to be handled
	case InternalTaskTypeSignal:
		f := workflow.SignalExternalWorkflow(localCtx, inputParam.Args[0], inputParam.Args[1],
			ReloadSignalName, inputParam.Args[2])

		err := f.Get(localCtx, nil)
		if err != nil {
			inputParam.Args = append(inputParam.Args, err.Error())
		} else {
			inputParam.Args = append(inputParam.Args, "Signal success")
		}
		err = workflow.ExecuteActivity(localCtx, a.Name, inputParam).Get(localCtx, nil)

	case InternalTaskTypePause:
		err = workflow.ExecuteActivity(localCtx, a.Name, inputParam).Get(localCtx, nil)
		ch := workflow.GetSignalChannel(localCtx, ReloadSignalName)
		selector := workflow.NewSelector(localCtx)
		selector.AddReceive(ch, func(c workflow.Channel, more bool) {})

		//wait until next DAG reload
		selector.Select(localCtx)

	// normal types
	default:

		if a.Count < 1 {
			a.Count = 1
		}

		selector := workflow.NewSelector(localCtx)

		for i := 0; i < a.Count; i++ {
			selector.AddFuture(workflow.ExecuteActivity(localCtx, a.Name, inputParam), func(f workflow.Future) {
				err = f.Get(localCtx, &result)
			})
		}

		countDown := a.Count
		for countDown > 0 {
			selector.Select(localCtx)
			countDown--
		}
	}

	// if pause, append a new pause task
	if a.Pause || (a.PauseOnFail && err != nil) {
		heartbeatCtx := workflow.WithActivityOptions(localCtx, workflow.ActivityOptions{
			WaitForCancellation: true,
			ScheduleToStartTimeout: 30 * 24 * time.Hour,
			StartToCloseTimeout:    30 * 24 * time.Hour,
			HeartbeatTimeout:       0,
		})
		workflow.ExecuteActivity(heartbeatCtx, "dsl/activityimpl.Pause", inputParam).Get(heartbeatCtx, nil)

	}

	// check if workflow func need to be canceled
	if err != nil {
		if ctx.Value(StopOnErr) == "true" {
			log.Printf("Calling cancel on %s", a.Name)
			cancelFunc()
		}
		return err
	}

	//export results
	if len(a.Results) > 0 {
		//copy all the keys to results
		for i, res := range a.Results {
			//compatible way to export single key
			bindingKey := replaceTemplateKey(res, ctx.Value(NodeNameKey).(string))
			bindings[getScopedKey(ctx.Value(ScopeKey).(string), bindingKey)] = result.Results[i]
		}
	} else if a.Result != "" {
		//compatible way to export single key
		bindingKey := replaceTemplateKey(a.Result, ctx.Value(NodeNameKey).(string))
		bindings[getScopedKey(ctx.Value(ScopeKey).(string), bindingKey)] = result.Results[0]
	}

	return nil
}

func (s *DagSequence) execute(ctx workflow.Context, cancelFunc workflow.CancelFunc, bindings map[string]string, nestLevel int, additionParams ...interface{}) error {
	var err error
	for _, a := range s.Elements {
		err = a.execute(ctx, cancelFunc, bindings, nestLevel, additionParams)
		if err != nil {
			return err
		}
	}
	return err
}

func executeAsync(exe dagExecutable, ctx workflow.Context, cancelFunc workflow.CancelFunc, bindings map[string]string, nestLevel int, additionParams ...interface{}) workflow.Future {
	future, settable := workflow.NewFuture(ctx)
	workflow.Go(ctx, func(ctx workflow.Context) {
		err := exe.execute(ctx, cancelFunc, bindings, nestLevel, additionParams)

		settable.Set(nil, err)
	})

	return future
}

func makeInput(ctx workflow.Context,  argNames []string, argsMap map[string]string, version workflow.Version) *ActivityInput {

	ret := new(ActivityInput)

	ret.NodeInfo = ctx.Value(NodeInfoKey).(string)
	scope := ctx.Value(ScopeKey).(string)

	var args []string
	for _, argKey := range argNames {
		argKey = replaceTemplateKey(argKey, ctx.Value(NodeNameKey).(string))
		args = append(args, argsMap[getScopedKey(scope, argKey)])
	}

	ret.Args = args
	ret.ActivityVersion = version
	return ret
}
