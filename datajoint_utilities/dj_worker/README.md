# DataJoint Worker

A robust pipeline execution manager for DataJoint workflows.

## Overview

The DataJoint Worker provides a mechanism to set up and manage "workers" that operate DataJoint pipelines. Each worker runs in a configurable loop that can:

- Execute DataJoint populate operations and custom functions
- Handle job reservations and error states
- Clean up stale jobs and error patterns
- Log worker activities and errors

NOTE: this is using the underlying datajoint's `.populate()` mechanim - we're not adding anything fancy here.

## Worker Lifecycle

The worker runs in a configurable loop that continues until one of these conditions is met:
1. Run duration exceeded: If `run_duration > 0` and elapsed time > `run_duration`
2. Max idle cycles exceeded: If `max_idled_cycle > 0` and consecutive idle cycles > `max_idled_cycle`

Each cycle of the loop:
1. Executes all registered processes in sequence
2. Handles any errors that occur during execution
3. Cleans up stale jobs and error patterns
4. Logs worker activities and errors
5. Sleeps for the configured duration before the next cycle

Note: When a stop condition is met, the worker will complete the current cycle (including all processes, error handling, and cleanup) before stopping. This ensures no jobs are left in an inconsistent state.

## Schema Structure

The worker maintains its own schema with tables for:
- `RegisteredWorker`: Tracks worker registration and configuration
- `WorkerLog`: Records worker activity and job processing
- `ErrorLog`: Stores error information for failed jobs

## Usage

### Basic Usage

```python
from datajoint_utilities.dj_worker import DataJointWorker

@DataJointWorker("my_worker", "worker_schema")
def my_process():
    # Your pipeline process here
    pass

# Run the worker
my_process.run()
```

### Advanced Configuration

```python
@DataJointWorker(
    "my_worker",
    "worker_schema",
    run_duration=3600,  # Run for 1 hour
    sleep_duration=60,  # Sleep 60 seconds between cycles
    max_idled_cycle=10,  # Stop after 10 idle cycles
    stale_timeout_hours=24,  # Consider jobs stale after 24 hours
    autoclear_error_patterns=["%Deadlock%", "%Lock wait timeout%"],
    db_prefix=["my_db"]
)
def my_process():
    # Your pipeline process here
    pass
```

### Imperative Usage

You can also use the worker in an imperative manner without decorators:

```python
from datajoint_utilities.dj_worker import DataJointWorker

# Create a worker instance
worker = DataJointWorker(
    "my_worker",
    "worker_schema",
    run_duration=3600,
    sleep_duration=60,
    stale_timeout_hours=24
)

# Add processes to the worker
worker(my_table1)  # Add a DataJoint table
worker(my_table2)  # Add another table
worker(my_custom_function)  # Add a custom function
worker(my_table3)  # Add another table

# Run the worker
worker.run()
```

### Stale Job Handling

The worker automatically handles stale jobs based on configurable time limits. A job is considered stale if it meets BOTH conditions:
1. Has been in "reserved" status for longer than the specified time limit (in hours)
2. The connection_id associated with the job is no longer active

This helps clean up jobs that may have been abandoned due to worker crashes or network issues while ensuring we don't remove jobs that are still being actively processed.



