# DAGWorkflow

DAGWorkflow is a dynamic, code-agnostic execution engine built on Uber Cadence that supports runtime DAG (Directed Acyclic Graph) topology mutations to execute highly volatile orchestration scenarios. It is built as a companion implementation for **US11307967B2 - Test orchestration platform**. Originally created 7 years ago as a personal Proof of Concept (PoC), it has been modernized and updated to demonstrate advanced, mutable workflow architectures.

The engine relies on Cadence (now Temporal-compatible) for fault-oblivious persistence, blending it with topological sorting and graph algorithms inspired by the Hashicorp Terraform core library to manage deeply complex task dependencies on the fly.

> [!NOTE]
> **Historical Context:** This repository was formulated circa ~2019 (approx. 7 years ago) prior to the explosive growth of the Temporal ecosystem. Today, you can find modern equivalents and native implementations of DSLs and DAG runners provided directly in the [official Temporal Go SDK samples](https://github.com/temporalio/samples-go/tree/main/dsl), as well as open-source frameworks like [iWF (interface Workflow)](https://github.com/indeedeng/iwf) which aim to provide higher-level declarative workflow abstractions natively on top of Cadence/Temporal. This repository stands as an early architectural demonstration of these concepts.


![DAG Example](./dsl-example.png)

## Key Features
- **Runtime Graph Resolution**: Execute complex parallel logic while resolving recursive and dynamic graph branches in-memory.
- **Dynamic Definition Reloading**: Signal a running workflow to intercept and swap its entire DAG definition payload strictly on-the-fly without losing current task state.
- **Parametrized Templating**: Use prototypes (`proto: true`) in YAML definitions to loop through and auto-generate new dependent nodes based on real-time activity constraints.
- **Generic Activity Bindings**: Effortlessly bind robust Go backend activities to dynamic DAG nodes using fluid declarative YAML configurations.

### Uniqueness vs. Modern DSLs (iWF, Serverless Workflow)
While modern frameworks like the Serverless Workflow specification or Temporal's iWF provide excellent declarative abstractions, this DAG implementation tackles highly dynamic edge cases that are typically rigid in other YAML engines:
* **Runtime DAG Mutation via `next_key`**: Activities can generate lists of next node *names* on the fly. The DAG topology actively recalculates and resolves transitive dependencies mid-flight rather than being statically compiled.
* **Templated Prototypes (`proto: true`)**: Nodes can be marked as prototypes, serving as factories for loops. The engine automatically scopes variables (`$$node_id`, `$$iterator`) giving you programmatic recursion natively within the YAML graph.
* **Dynamic Scoping**: State context is isolated through explicitly defined boundary namespaces (`test.flipcoin2`), preventing variable collisions in complex parallel branching.

### Unique Use Case: Hardware Diagnostic Orchestration
Imagine a hardware testing suite (aligning with the **US11307967B2** patent) where a master orchestrator runs diagnostic arrays against newly manufactured devices. Standard static DAGs fail here because the required subsequent tests are *entirely unknown* until the initial diagnostic reports hardware variances. 

With **DAGWorkflow**, the initial diagnostic activity can return an array of subsequent test node names (`next_key`), injecting brand new sub-graphs into the ongoing process dynamically. If a voltage anomaly is detected, the workflow topologically spawns new scoped diagnostic branches (`proto: true`) recursively until the hardware passes or is rejected. This executes cleanly within a single, persistent workflow entity.

---

## 🛠 Setup & Integration Testing

To run the full E2E environment, you need a Cadence server and a Go worker.

### 1. Start Cadence Stack
The easiest way to start the backend (Cadence + Cassandra + Web UI) is via Docker Compose:

```bash
git clone https://github.com/cadence-workflow/cadence.git
cd cadence/docker
docker-compose up -d
```

### 2. Build the DSL Worker
Requires Go 1.22+.

```bash
cd src/dsl
go mod tidy
go build -o dsl ./
```

### 3. Run the Worker
The worker registers the activities and listens for workflow tasks.

```bash
./dsl -m worker -config config/development.yaml
```

### 4. Trigger a Workflow
In a separate terminal, submit a DAG configuration to the engine:

```bash
./dsl -m trigger -dslConfig samples/workflow_result.yaml
```

---

## 🧪 Scenarios

| Workflow | Description |
|---|---|
| `workflow_dag.yaml` | Standard sequence/parallel execution with coin-flip logic. |
| `workflow_job.yaml` | Generic monitoring and retry flow for external jobs. |
| `workflow_result.yaml` | **(New)** Demonstration of non-null result extraction. |
| `workflow_dag_sleep.yaml` | Handling timers and delays within the graph. |

*Note: Legacy `SimpleDSLWorkflow` components have been moved to the `src/dsl/archive` directory.*

---

## 📜 Acknowledgments
Developed as a personal demonstration of distributed orchestration patterns and as a companion to the architectural concepts in US Patent 11,307,967.
