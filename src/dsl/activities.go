package main

import (
	"fmt"
	"strconv"
	"time"
	"context"
	"math/rand"
	"github.com/pkg/errors"
	"go.uber.org/cadence/activity"
	"encoding/json"
	"log"
	"dsl/workflow/dag_workflow"
	"go.uber.org/cadence/workflow"
	"strings"
)

func sampleActivity1(ctx context.Context, input dag_workflow.ActivityInput) (*dag_workflow.ActivityOutput, error) {
	name := "sampleActivity1"
	fmt.Printf("Run %s with input %v \n", name, input)

	return &dag_workflow.ActivityOutput{Results:[]string{"Result_" + name}}, nil
}

func signalActivity(ctx context.Context, input dag_workflow.ActivityInput) (*dag_workflow.ActivityOutput, error) {
	return  &dag_workflow.ActivityOutput{Results:[]string{fmt.Sprintf("Signal workflow result: %+v", input)}}, nil
}

func pauseActivity(ctx context.Context, input dag_workflow.ActivityInput) (*dag_workflow.ActivityOutput, error) {
	return &dag_workflow.ActivityOutput{Results:[]string{fmt.Sprintf("Pause workflow result: %+v", input)}}, nil
}

func dynamicActivity(ctx context.Context, input dag_workflow.ActivityInput) (*dag_workflow.ActivityOutput, error) {
	var ret []string

	ret = input.Args

	retStr, _ := json.Marshal(ret)
	log.Printf("Dynamic activity result: %s", retStr)
	return &dag_workflow.ActivityOutput{Results:[]string{string(retStr)}}, nil
}

func dynamicOnFalseConditionActivity(ctx context.Context, input dag_workflow.ActivityInput) (*dag_workflow.ActivityOutput, error) {

	var ret []string

	if input.Args[0] == "false" {
		//replace iterator
		i := 0
		for _, nodeName := range input.Args[1:] {
			if strings.Contains(nodeName, "$$iterator") {
				nodeName = strings.ReplaceAll(nodeName, "$$iterator", fmt.Sprint(i))
				i++
			}

			ret = append(ret, nodeName)
		}
	}



	retStr, _ := json.Marshal(ret)
	log.Printf("Dynamic activity %+v result: %s", input.Args, retStr)
	return &dag_workflow.ActivityOutput{Results:[]string{string(retStr)}}, nil

}

func flipCoin(ctx context.Context, input dag_workflow.ActivityInput) (*dag_workflow.ActivityOutput, error) {
	res := coinFlip()
	var err error = nil

	go func() {
		t := time.NewTicker(10 * time.Second);
		defer t.Stop();
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				activity.RecordHeartbeat(ctx, true)
			}
		}
	}()

	fmt.Printf("[%s] Run %s with input %v and res %v for wf %s \n", time.Now().Format(time.RFC3339), res, input, res, activity.GetInfo(ctx).WorkflowExecution.ID)

	var duration time.Duration
	if !res {
		duration = time.Second * time.Duration(rand.Int63() % 30)
	} else {
		duration = time.Duration(25 + rand.Int63() % 5) * time.Second
	}

	select {
	case <- ctx.Done():
		log.Printf("Activity %s context cancelled", input.NodeInfo)
		return &dag_workflow.ActivityOutput{Results:[]string{""}}, workflow.ErrCanceled
	case <- time.NewTimer(duration).C:
		return &dag_workflow.ActivityOutput{Results:[]string{fmt.Sprint(res)}}, err

	}

}

func sampleVersionActivity(ctx context.Context, input dag_workflow.ActivityInput) (*dag_workflow.ActivityOutput, error) {
	name := "sampleVersionActivity"

	if input.ActivityVersion != workflow.DefaultVersion && input.ActivityVersion < 2 {
		return &dag_workflow.ActivityOutput{Results:[]string{""}}, errors.New("version not supported")
	}

	res := coinFlip();
	var err error = nil;
	go func() {
		t := time.NewTicker(10 * time.Second);
		defer t.Stop();
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				activity.RecordHeartbeat(ctx, true)
			}
		}
	}()

	fmt.Printf("[%s] Run %s with input %v and res %v for wf %s \n", time.Now().Format(time.RFC3339), name, input, res, activity.GetInfo(ctx).WorkflowExecution.ID)

	if !res {
		time.Sleep(11 * time.Second);
		err = errors.New("failed in activity 2")
	} else {
		time.Sleep(1 * time.Second);
	}

	return &dag_workflow.ActivityOutput{Results:[]string{"Result_" + name}}, err
}

func sampleActivity2(ctx context.Context, input dag_workflow.ActivityInput) (*dag_workflow.ActivityOutput, error) {
	name := "sampleActivity2"

	res := coinFlip();
	var err error = nil;
	go func() {
		t := time.NewTicker(10 * time.Second);
		defer t.Stop();
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				activity.RecordHeartbeat(ctx, true)
			}
		}
	}()

	fmt.Printf("[%s] Run %s with input %v and res %v for wf %s \n", time.Now().Format(time.RFC3339), name, input, res, activity.GetInfo(ctx).WorkflowExecution.ID)

	if !res {
		time.Sleep(11 * time.Second);
		err = errors.New("failed in activity 2")
	} else {
		time.Sleep(1 * time.Second);
	}

	return &dag_workflow.ActivityOutput{Results:[]string{"Result_" + name}}, err
}

// not work well in concurrent mode because of using same bindings without locking
func breakOnMaxNRevisitActivity(ctx context.Context, input dag_workflow.ActivityInput) (*dag_workflow.ActivityOutput, error) {
	currentCountStr := input.Args[1]
	currentCount, err := strconv.ParseInt(currentCountStr, 10, 0)
	if err != nil {
		return &dag_workflow.ActivityOutput{Results:[]string{""}}, errors.New("failed to load current revisit count")
	}
	maxCountStr := input.Args[0]
	maxCount, err := strconv.ParseInt(maxCountStr, 10, 0)
	if err != nil {
		return &dag_workflow.ActivityOutput{Results:[]string{""}}, errors.New("failed to load maximum count")
	}
	currentCount++
	if currentCount > maxCount {
		return &dag_workflow.ActivityOutput{Results:[]string{""}}, errors.New("exceed maximum revisit")
	}
	return &dag_workflow.ActivityOutput{Results:[]string{strconv.FormatInt(currentCount, 10)}}, nil
}

func sampleActivity3(input dag_workflow.ActivityInput) (*dag_workflow.ActivityOutput, error) {
	name := "sampleActivity3"
	fmt.Printf("Run %s with input %v \n", name, input)
	return &dag_workflow.ActivityOutput{Results:[]string{"Result_" + name}}, nil
}

func sampleActivity4(input dag_workflow.ActivityInput) (*dag_workflow.ActivityOutput, error) {
	name := "sampleActivity4"
	fmt.Printf("Run %s with input %v \n", name, input)
	return &dag_workflow.ActivityOutput{Results:[]string{"Result_" + name}}, nil
}

func sampleActivity5(input dag_workflow.ActivityInput) (*dag_workflow.ActivityOutput, error) {
	name := "sampleActivity5"
	fmt.Printf("Run %s with input %v \n", name, input)
	return &dag_workflow.ActivityOutput{Results:[]string{"Result_" + name}}, nil
}


//util func
func coinFlip() bool {

	if rand.Int()%2 > 0 {
		return true;
	} else {
		return false;
	}
}
