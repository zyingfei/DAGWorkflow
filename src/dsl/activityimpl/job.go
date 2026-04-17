package activityimpl

import (
	"context"
	"fmt"
	"go.uber.org/cadence/activity"
	"math/rand"
	"time"
	"errors"
	"dsl/workflow/dag_workflow"
)

type JobDetail struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	Message     string `json:"message"`
}

// RandomNumberToZero is a helper for simulation
func RandomNumberToZero(timeoutStr string) (bool, error) {
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return false, nil
	}
	result := false
	fmt.Printf("Random number interval is 1s, timeout is %v\n", timeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var randomNumber int
	timeIsOut := false

	c1 := make(chan bool, 1)
	go func() {
		time.Sleep(timeout)
		c1 <- true
	}()

	for {
		select {
		case <-ticker.C:
			randomNumber = rand.Intn(10)
			fmt.Println(randomNumber)
		case <-c1:
			fmt.Println("Times up, return false.")
			timeIsOut = true
		}

		if randomNumber == 0 {
			result = true
			break
		}

		if timeIsOut == true {
			break
		}
	}

	return result, nil
}

// MonitorJob takes jobId and frequency as arguments.
// It simulates monitoring a job and returns its status.
func MonitorJob(ctx context.Context, input dag_workflow.ActivityInput) (*dag_workflow.ActivityOutput, error) {
	jobId := input.Args[0]

	if jobId == "" {
		return &dag_workflow.ActivityOutput{Results: []string{""}}, errors.New("JobId should not be empty")
	}

	frequencyStr := input.Args[1]
	timeoutStr := input.Args[2]

	frequency, err := time.ParseDuration(frequencyStr)
	if err != nil {
		activity.GetLogger(ctx).Error("Bad monitor frequency input")
		return &dag_workflow.ActivityOutput{Results: []string{""}}, err
	}
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		activity.GetLogger(ctx).Error("Bad monitor timeout input")
		return &dag_workflow.ActivityOutput{Results: []string{""}}, err
	}

	ticker := time.NewTicker(frequency)
	defer ticker.Stop()
	
	timeoutCh := time.After(timeout)
	
	var jobRes JobDetail
	for {
		select {
		case <-ticker.C:
			// Simulate a response
			jobRes = JobDetail{
				ID:     jobId,
				Status: "Running",
			}
			// In a real scenario, you'd call an API here.
			// For simulation, we randomly succeed or fail.
			if rand.Intn(10) < 2 {
				jobRes.Status = "Completed"
			} else if rand.Intn(10) < 1 {
				jobRes.Status = "Failed"
			}
			
			activity.RecordHeartbeat(ctx, jobRes)
		case <-timeoutCh:
			return &dag_workflow.ActivityOutput{Results: []string{"timeout"}}, nil
		}

		if jobRes.Status == "Completed" {
			return &dag_workflow.ActivityOutput{Results: []string{"success"}}, nil
		}
		if jobRes.Status == "Failed" {
			return &dag_workflow.ActivityOutput{Results: []string{"retry"}}, nil
		}
	}
}

// RetryJob simulates retrying a job if it failed.
func RetryJob(ctx context.Context, input dag_workflow.ActivityInput) (*dag_workflow.ActivityOutput, error) {
	jobId := input.Args[0]
	monitorResult := input.Args[1]

	if monitorResult == "success" {
		return &dag_workflow.ActivityOutput{Results: []string{"break"}}, nil
	}
	if monitorResult != "retry" {
		return &dag_workflow.ActivityOutput{Results: []string{jobId}}, nil
	}

	fmt.Printf("Job %s retried successfully\n", jobId)
	return &dag_workflow.ActivityOutput{Results: []string{jobId + "-retry"}}, nil
}
