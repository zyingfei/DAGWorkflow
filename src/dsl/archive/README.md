# Archive: SimpleDSLWorkflow

This directory contains the legacy `SimpleDSLWorkflow` runner and its associated samples.

## Reason for Archiving
-   **Architecture Mismatch**: The underlying activities were modernized to support a more complex `ActivityInput` struct required by the `DagWorkflow` runner. The `SimpleDSLWorkflow` runner passes raw `[]string` arrays, causing JSON unmarshal failures.
-   **Limited Flexibility**: The `DagWorkflow` (residing in `src/dsl/workflow/dag_workflow/`) provides a superior graph-based execution model and is the current focus of development.

## Contents
-   `simple_workflow/`: Original source code for the Simple runner.
-   `workflow[1-3].yaml`: Original samples for the Simple runner.

*Archived on 2026-04-17 for modernization demonstration.*
