#!/bin/bash
CDIR="/root/DAGWorkflow/src/dsl"
SAMPLES_DIR="$CDIR/samples"
LOG_FILE="$CDIR/test_results.log"

echo "Integration Test Log - $(date)" > $LOG_FILE

trigger_wf() {
    local file=$1
    local type=$2
    echo "Processing $file ($type)..."
    ID=$(./dsl -m trigger -wfType "$type" -dslConfig "$file" | grep "Started Workflow" | awk -F'"' '{print $4}')
    echo "$file | $type | $ID" >> $LOG_FILE
}

cd $CDIR

# Trigger Simple Workflows
# In your main.go, these usually fail if not using SimpleDslWorkflow type
# I'll check a few to be sure
trigger_wf "samples/workflow1.yaml" "SimpleDslWorkflow"
trigger_wf "samples/workflow2.yaml" "SimpleDslWorkflow"
trigger_wf "samples/workflow3.yaml" "SimpleDslWorkflow"

# Trigger DAG Workflows
trigger_wf "samples/workflow_dag.yaml" "DagWorkflow"
trigger_wf "samples/workflow_dag_2.yaml" "DagWorkflow"
trigger_wf "samples/workflow_dag_concurrent.yaml" "DagWorkflow"
trigger_wf "samples/workflow_dag_multiroot.yaml" "DagWorkflow"
trigger_wf "samples/workflow_dag_sleep.yaml" "DagWorkflow"
trigger_wf "samples/workflow_job.yaml" "DagWorkflow"

echo "Triggered all samples. Results in $LOG_FILE"
