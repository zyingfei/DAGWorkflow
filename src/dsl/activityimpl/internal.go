package activityimpl

import (
	"context"
	"go.uber.org/cadence/activity"
	"dsl/workflow/dag_workflow"
)


func Pause(ctx context.Context, input dag_workflow.ActivityInput) (*dag_workflow.ActivityOutput, error) {
	//token := activity.GetInfo(ctx).TaskToken
	return &dag_workflow.ActivityOutput{Results:[]string{"Paused"}}, activity.ErrResultPending
}