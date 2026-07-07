# Overview

## Purpose

Coppice is a distributed batch job scheduler for running containerized workloads
across a fleet of compute nodes. Users submit jobs as Docker images with
resource requirements, placement constraints, priorities, quotas, and runtime
metadata. The system schedules those jobs onto nodes, supervises execution,
tracks state, exposes APIs and streaming updates, and remains available despite
coordinator failures.

The design targets approximately:

- 1,000 compute nodes.
- Up to 100 concurrent jobs per node.
- Approximately 1 million queued jobs.
- Job durations from roughly 1 minute to 1 day.
- Multi-resource scheduling across CPU, memory, disk, and future resource types
  such as GPUs.
- High-availability control plane using a replicated consensus system.
- Strong observability through metrics, logs, events, and audit trails.

The scheduler is intended to be a batch-oriented system, not a low-latency
serverless platform. Throughput, fairness, correctness, debuggability, and
failure recovery are more important than millisecond-level scheduling latency.

## Core responsibilities

The system has several primary responsibilities.

It accepts job submissions from users and services, validates them, records them
durably, and exposes their lifecycle state.

It maintains an authoritative view of cluster state, including jobs, nodes,
allocations (including accruing allocations that earmark future capacity),
quotas, and relevant scheduling metadata.

It makes placement decisions using a scheduler that understands resource
availability, hard constraints, soft preferences, quotas, priorities, affinity,
anti-affinity, image locality, and other policies.

It dispatches work to node agents, supervises execution indirectly through those
agents, and reconciles observed state with intended state.

It monitors actual resource usage and job progress where available, while
keeping high-volume telemetry out of the strongly replicated control-plane state
unless it is semantically important.

It supports push-style subscriptions for clients interested in state changes for
a subset of jobs.

It provides a web UI and API for submission, monitoring, administration,
debugging, and operational control.

It remains available through coordinator replication and leader failover.

## Where to go next

- The moving parts are described in [architecture/components.md](architecture/components.md).
- The principles that hold the design together are in [design-principles.md](design-principles.md).
- What the first implementation should cover is in [roadmap/initial-scope.md](roadmap/initial-scope.md).
