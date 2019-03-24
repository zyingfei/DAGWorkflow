package activityimpl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/cadence/activity"
	"math/rand"
	"net/http"
	"time"
	"errors"
	"dsl/workflow/dag_workflow"
)

type TpaasJobDetail struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	NumDifCases int    `json:"num_dif_cases"`
}

type ResubmitBody struct {
}


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

func MonitorTpaasJob(ctx context.Context, input dag_workflow.ActivityInput) (*dag_workflow.ActivityOutput, error) {
	token := input.Args[0]
	jobId := input.Args[1]

	if token == "" || jobId == "" {
		return &dag_workflow.ActivityOutput{Results:[]string{""}}, errors.New("JobId or token should not be empty")
	}

	frequencyStr := input.Args[2]
	timeoutStr := input.Args[3]
	//parse time duration paras
	frequency, err := time.ParseDuration(frequencyStr)
	if err != nil {
		activity.GetLogger(ctx).Error("Bad monitor frequency input")
		return &dag_workflow.ActivityOutput{Results:[]string{""}}, err
	}
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		activity.GetLogger(ctx).Error("Bad monitor timeout input")
		return &dag_workflow.ActivityOutput{Results:[]string{""}}, err
	}
	//form request and create client
	endpoint := "https://tpaas.oraclecorp.com/api/v1/tests/" + jobId
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		activity.GetLogger(ctx).Error("Job monitor failed because of forming request")
		return &dag_workflow.ActivityOutput{Results:[]string{""}}, err
	}
	req.Header.Add("x-auth-token", token)
	client := &http.Client{Timeout: 10 * time.Second}

	ticker := time.NewTicker(frequency)
	defer ticker.Stop()
	timeIsOut := false

	timeoutCh := make(chan bool, 1)
	go func() {
		time.Sleep(timeout)
		timeoutCh <- true
	}()
	var tpaasRes TpaasJobDetail
	for {
		select {
		case <-ticker.C:
			err := getResponse(client, req, &tpaasRes)
			if err != nil {
				activity.GetLogger(ctx).Error(fmt.Sprintf("Job monitor failed : %v", err))
				return &dag_workflow.ActivityOutput{Results:[]string{""}}, err
			}
			activity.RecordHeartbeat(ctx, tpaasRes)
		case <-timeoutCh:
			fmt.Println("Time is up for monitoring.")
			timeIsOut = true
		}

		if timeIsOut == true {
			break
		}
		if tpaasRes.Status == "Completed" {
			if tpaasRes.NumDifCases == 0 {
				return &dag_workflow.ActivityOutput{Results:[]string{"success"}}, nil
			} else {
				return &dag_workflow.ActivityOutput{Results:[]string{"retry"}}, nil
			}
		}
		if tpaasRes.Status == "Initialize-Failed" {
			return &dag_workflow.ActivityOutput{Results:[]string{"initialize-failed"}}, nil
		}
	}

	return &dag_workflow.ActivityOutput{Results:[]string{"timeout"}}, nil
}

func RetryTpaasJob(ctx context.Context, input dag_workflow.ActivityInput) (*dag_workflow.ActivityOutput, error) {
	token := input.Args[0]
	jobId := input.Args[1]
	monitorResult := input.Args[2]
	if monitorResult == "success" || monitorResult == "initialize-failed" {
		return &dag_workflow.ActivityOutput{Results:[]string{"break"}}, nil
	}
	if monitorResult != "retry" {
		return &dag_workflow.ActivityOutput{Results:[]string{jobId}}, nil
	}
	endpoint := "https://tpaas.oraclecorp.com/api/v3/tests/" + jobId + "/resubmit"
	body, _ := json.Marshal(ResubmitBody{})
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		activity.GetLogger(ctx).Info("Job monitor failed because of forming request")
		return &dag_workflow.ActivityOutput{Results:[]string{""}}, err
	}
	req.Header.Add("x-auth-token", token)
	req.Header.Add("Content-Type", "application/json")

	client := new(http.Client)
	client.Timeout = 10 * time.Second
	resp, err := client.Do(req)
	defer resp.Body.Close()
	if err != nil {
		activity.GetLogger(ctx).Info("Job monitor failed because of sending request")
		return &dag_workflow.ActivityOutput{Results:[]string{""}}, err
	}

	var tpaasRes TpaasJobDetail
	err = json.NewDecoder(resp.Body).Decode(&tpaasRes)
	if err != nil {
		activity.GetLogger(ctx).Info("Job monitor failed because of unexpected response")
		return &dag_workflow.ActivityOutput{Results:[]string{""}}, err
	}
	fmt.Printf("Job resumbit successfully, job Id is : %s\n", tpaasRes.ID)
	return &dag_workflow.ActivityOutput{Results:[]string{tpaasRes.ID}}, nil
}

func getResponse(client *http.Client, req *http.Request, jobDetail *TpaasJobDetail) error {
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(jobDetail)
	if err != nil {
		return err
	}
	fmt.Println(jobDetail)
	return nil
}
