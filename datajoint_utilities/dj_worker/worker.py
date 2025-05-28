"""
DataJoint Worker - A robust pipeline execution manager for DataJoint workflows.

This module provides a mechanism to set up and manage "workers" that operate DataJoint pipelines.
Each worker runs in a configurable loop that can:
- Execute DataJoint populate operations and custom functions
- Handle job reservations and error states
- Clean up stale jobs and error patterns
- Log worker activities and errors

Key Features:
- Configurable run duration and sleep intervals
- Automatic handling of stale jobs with time-based detection
- Comprehensive logging of worker activities and errors
- Backward compatibility with existing configurations

The worker maintains its own schema with tables for:
- RegisteredWorker: Tracks worker registration and configuration
- WorkerLog: Records worker activity and job processing
- ErrorLog: Stores error information for failed jobs

Example:
    @DataJointWorker("my_worker", "worker_schema")
    def my_process():
        # Your pipeline process here
        pass

    # Run the worker
    my_process.run()
"""

import inspect
import time
import warnings
from datetime import datetime
import datajoint as dj

from .. import dict_to_uuid
from .worker_schema import (
    RegisteredWorker,
    WorkerLog,
    ErrorLog,
    get_process_name,
    is_djtable,
)

logger = dj.logger

_populate_settings = {
    "display_progress": True,
    "reserve_jobs": True,
    "suppress_errors": True,
}

RETURN_SUCCESS_COUNT = dj.__version__ > "0.14.0"


class DataJointWorker:
    """
    A decorator class for running and managing DataJoint populate jobs.

    The worker runs in a configurable loop that continues until one of these conditions is met:
    1. Run duration exceeded: If run_duration > 0 and elapsed time > run_duration
    2. Max idle cycles exceeded: If max_idled_cycle > 0 and consecutive idle cycles > max_idled_cycle

    Each cycle of the loop:
    1. Executes all registered processes in sequence
    2. Handles any errors that occur during execution
    3. Cleans up stale jobs and error patterns
    4. Logs worker activities and errors
    5. Sleeps for the configured duration before the next cycle

    Note: When a stop condition is met, the worker will complete the current cycle (including
    all processes, error handling, and cleanup) before stopping. This ensures no jobs are
    left in an inconsistent state.

    The worker maintains its own schema with tables for:
    - RegisteredWorker: Tracks worker registration and configuration
    - WorkerLog: Records worker activity and job processing
    - ErrorLog: Stores error information for failed jobs
    """

    def __init__(
        self,
        worker_name: str,
        worker_schema_name: str,
        *,
        run_duration: int = -1,
        sleep_duration: int = 60,
        max_idled_cycle: int = -1,
        autoclear_error_patterns: list[str] = [],
        db_prefix: list[str] = [""],
        stale_timeout_hours: int = 24,
        remove_stale_reserved_jobs: bool = None,  # For backward compatibility
    ):
        """
        Initialize a DataJoint worker to manage and execute pipeline processes.

        A worker is responsible for:
        1. Managing the execution of DataJoint populate operations and custom functions
        2. Handling job reservations and error states
        3. Cleaning up stale jobs and error patterns
        4. Logging worker activities and errors

        Args:
            worker_name (str): Unique identifier for this worker instance
            worker_schema_name (str): Name of the schema where worker-related tables are stored
            run_duration (int, optional): Maximum runtime in seconds (-1 for unlimited). Defaults to -1.
            sleep_duration (int, optional): Time to wait between processing cycles in seconds. Defaults to 60.
            max_idled_cycle (int, optional): Maximum number of consecutive cycles with no successful jobs (-1 for unlimited). Defaults to -1.
            autoclear_error_patterns (list[str], optional): List of error message patterns to automatically clear. Defaults to [].
            db_prefix (list[str], optional): Prefix(es) for database names when logging errors. Defaults to [""].
            stale_timeout_hours (int, optional): Time in hours after which a reserved job is considered stale. Defaults to 24.
            remove_stale_reserved_jobs (bool, optional): [DEPRECATED] Use stale_timeout_hours instead. Defaults to None.

        Note:
            The worker maintains its own schema with tables for:
            - RegisteredWorker: Tracks worker registration and configuration
            - WorkerLog: Records worker activity and job processing
            - ErrorLog: Stores error information for failed jobs
        """
        self.name = worker_name
        self._worker_schema = dj.schema(worker_schema_name)
        self._worker_schema(RegisteredWorker)
        self._worker_schema(WorkerLog)
        self._worker_schema(ErrorLog)

        self._autoclear_error_patterns = autoclear_error_patterns
        self._run_duration = run_duration if run_duration is not None else -1
        self._sleep_duration = sleep_duration
        self._max_idled_cycle = max_idled_cycle if RETURN_SUCCESS_COUNT else -1
        self._db_prefix = [db_prefix] if isinstance(db_prefix, str) else db_prefix

        # Handle backward compatibility
        if remove_stale_reserved_jobs is not None:
            warnings.warn(
                "The 'remove_stale_reserved_jobs' parameter is deprecated and will be removed in a future version. "
                "Use 'stale_timeout_hours' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            # If remove_stale_reserved_jobs is False, set time limit to 0 (disabled)
            # If True, keep the default 24 hours
            self._stale_timeout_hours = (
                stale_timeout_hours if remove_stale_reserved_jobs else 0
            )
        else:
            self._stale_timeout_hours = stale_timeout_hours

        self._processes_to_run = []
        self._pipeline_modules = {}
        self._idled_cycle_count = None
        self._run_start_time = None
        self._is_registered = False

    def __call__(self, process, **kwargs):
        self.add_step(process, **kwargs)

    def add_step(self, callable: callable, position_: int = None, **kwargs) -> None:
        """
        Add a new process to the list of processes to be executed by this worker.

        This method supports two types of processes:
        1. DataJoint AutoPopulate tables
        2. Custom functions or methods

        Args:
            callable (callable): The process to add. Can be either:
                - A DataJoint AutoPopulate table
                - A function or method
            position_ (int, optional): Position to insert the process in the execution order.
                If None, appends to the end. Defaults to None.
            **kwargs: Additional keyword arguments to pass to the process when executed.

        Raises:
            NotImplemented: If the callable is neither a DataJoint table nor a function/method.
            AssertionError: If a DataJoint table is not of type AutoPopulate.

        Note:
            - For DataJoint tables, the schema must be accessible
            - Adding a process resets the worker's registration status
        """
        index = len(self._processes_to_run) if position_ is None else position_
        if is_djtable(callable):
            assert is_djtable(
                callable, dj.user_tables.AutoPopulate
            ), f"Table '{callable.__name__}' is not of type AutoPopulate table - unable to add to worker"
            schema_name = callable.database
            if not schema_name:
                return
            self._processes_to_run.insert(index, ("dj_table", callable, kwargs))
            if schema_name not in self._pipeline_modules:
                self._pipeline_modules[schema_name] = dj.create_virtual_module(
                    schema_name, schema_name
                )
        elif inspect.isfunction(callable) or inspect.ismethod(callable):
            self._processes_to_run.insert(index, ("function", callable, kwargs))
        else:
            raise NotImplemented(
                f"Unable to handle processing step of type {type(callable)}"
            )
        self._is_registered = False

    def register_worker(self) -> None:
        """
        Register the worker and its associated processes in the RegisteredWorker table.

        This method performs the following operations:
        1. Creates process entries for each registered process, including:
           - Process name and index
           - Full table name (for DataJoint tables)
           - Key source SQL (for DataJoint tables)
           - Process configuration and arguments
        2. Generates a unique configuration UUID for the worker
        3. Registers the worker with its configuration in the database
        4. Handles transaction safety and error logging

        The registration process ensures that:
        - Each worker has a unique configuration
        - All processes are properly tracked
        - Configuration changes are detected
        - Worker state is persisted in the database

        Note:
            - If the worker is already registered with the same configuration, this method does nothing
            - If the worker exists with a different configuration, the old registration is deleted
            - All operations are performed within a transaction for data consistency
        """
        if self._is_registered:
            return

        process_entries = []
        for index, (process_type, process, kwargs) in enumerate(self._processes_to_run):
            entry = {
                "worker_name": self.name,
                "process": get_process_name(process, self._db_prefix),
                "process_index": index,
                "full_table_name": (
                    process.full_table_name if process_type == "dj_table" else ""
                ),
                "key_source_sql": (
                    process.key_source.proj().make_sql()
                    if process_type == "dj_table"
                    else None
                ),
                "process_kwargs": kwargs,
            }
            entry["process_config_uuid"] = dict_to_uuid(entry)
            process_entries.append(entry)

        worker_config_uuid = dict_to_uuid(
            {e["process_index"]: e["process_config_uuid"] for e in process_entries}
        )
        if RegisteredWorker & {
            "worker_name": self.name,
            "worker_config_uuid": worker_config_uuid,
        }:
            return

        worker_entry = {
            "worker_name": self.name,
            "registration_time": datetime.utcnow(),
            "worker_kwargs": {
                "worker_name": self.name,
                "worker_schema_name": self._worker_schema.database,
                "run_duration": self._run_duration,
                "sleep_duration": self._sleep_duration,
                "max_idled_cycle": self._max_idled_cycle,
                "autoclear_error_patterns": self._autoclear_error_patterns,
                "db_prefix": self._db_prefix,
                "stale_timeout_hours": self._stale_timeout_hours,
            },
            "worker_config_uuid": worker_config_uuid,
        }

        with RegisteredWorker.connection.transaction:
            with dj.config(safemode=False):
                (RegisteredWorker & {"worker_name": self.name}).delete()
            RegisteredWorker.insert1(worker_entry)
            RegisteredWorker.Process.insert(process_entries)

        self._is_registered = True
        logger.info(f"Worker registered: {self.name}")

    def _run_once(self) -> int:
        """
        Execute all registered processes in sequence, once.

        This method:
        1. Logs the start of each process
        2. Executes DataJoint populate operations or custom functions
        3. Handles any errors that occur during execution
        4. Cleans up stale jobs and error patterns
        5. Manages log rotation

        Returns:
            int: Number of successfully processed jobs. Returns 1 if RETURN_SUCCESS_COUNT is False.

        Note:
            - For DataJoint tables, uses populate() with standard settings
            - For functions, executes directly and logs any exceptions
            - Cleans up old logs after execution
        """
        success_count = 0 if RETURN_SUCCESS_COUNT else 1
        for process_type, process, kwargs in self._processes_to_run:
            WorkerLog.log_process_job(
                process, worker_name=self.name, db_prefix=self._db_prefix
            )
            if process_type == "dj_table":
                status = process.populate(**{**_populate_settings, **kwargs})
                if isinstance(status, dict):
                    success_count += status["success_count"]
            elif process_type == "function":
                try:
                    process(**kwargs)
                except Exception as e:
                    if hasattr(e, "key") and isinstance(e.key, dict):
                        key = e.key
                    else:
                        key = dict(error_time=datetime.utcnow())
                    ErrorLog.log_exception(key, process, str(e))

        _clean_up(
            self._pipeline_modules.values(),
            additional_error_patterns=self._autoclear_error_patterns,
            db_prefix=self._db_prefix,
            stale_timeout_hours=self._stale_timeout_hours,
        )

        WorkerLog.delete_old_logs()
        ErrorLog.delete_old_logs()

        return success_count

    def _keep_running(self) -> bool:
        """
        Determine whether the worker should continue running based on configured limits.

        The worker will stop if ANY of these conditions are met:
        1. Run duration exceeded: If run_duration > 0 and elapsed time > run_duration
        2. Max idle cycles exceeded: If max_idled_cycle > 0 and consecutive idle cycles > max_idled_cycle

        Returns:
            bool: True if the worker should continue running, False otherwise
        """
        exceed_run_duration = not (
            time.time() - self._run_start_time < self._run_duration
            or self._run_duration < 0
        )
        exceed_max_idled_cycle = 0 < self._max_idled_cycle < self._idled_cycle_count
        return not (exceed_run_duration or exceed_max_idled_cycle)

    def _purge_invalid_jobs(self) -> None:
        """
        Remove invalid or outdated jobs from the job tables.

        This method checks and removes jobs that are:
        1. No longer in the key_source (e.g., upstream entries were deleted)
        2. Already present in the target table (e.g., completed by another process)

        Note:
            - This is a potentially time-consuming operation
            - Should be run infrequently
            - Logs the number of invalid jobs removed
        """
        for process_type, process, _ in self._processes_to_run:
            if process_type == "dj_table":
                vmod = self._pipeline_modules[process.database]
                purge_invalid_jobs(vmod.schema.jobs, process)

    def run(self) -> None:
        """
        Run all registered processes in a continuous loop until stop conditions are met.

        This method:
        1. Registers the worker if not already registered
        2. Enters a loop that:
           - Executes all processes
           - Handles errors
           - Cleans up stale jobs
           - Logs activities
           - Sleeps between cycles
        3. Continues until stop conditions are met
        4. Purges invalid jobs before stopping

        The worker will stop when:
        - Run duration is exceeded (if configured)
        - Max idle cycles are exceeded (if configured)
        - An unhandled exception occurs

        Note:
            - The worker completes the current cycle before stopping
            - All operations are logged
            - Invalid jobs are purged before stopping
        """
        self.register_worker()

        logger.info(f"Starting DataJoint Worker: {self.name}")
        self._run_start_time = time.time()
        self._idled_cycle_count = 0
        while self._keep_running():
            try:
                success_count = self._run_once()
            except Exception as e:
                logger.error(str(e))
            else:
                self._idled_cycle_count += bool(not success_count)
            time.sleep(self._sleep_duration)

        self._purge_invalid_jobs()
        logger.info(f"Stopping DataJoint Worker: {self.name}")


def _clean_up(
    pipeline_modules,
    additional_error_patterns=[],
    db_prefix="",
    stale_timeout_hours: int = 24,  # Default 24 hours timeout
):
    """
    Routine to clear entries from the jobs table that are:
    + generic-type error jobs
    + stale "reserved" jobs
        Stale "reserved" jobs are jobs that meet BOTH conditions:
        1. Have been in "reserved" status for longer than the specified time limit (in hours)
        2. The connection_id associated with the job is no longer active
        This helps clean up jobs that may have been abandoned due to worker crashes or network issues
        while ensuring we don't remove jobs that are still being actively processed.
    """
    _generic_errors = [
        "%Deadlock%",
        "%Lock wait timeout%",
        "%MaxRetryError%",
        "%KeyboardInterrupt%",
        "InternalError: (1205%",
        "%SIGTERM%",
        "%LostConnectionError%",
    ]

    for pipeline_module in pipeline_modules:
        # clear generic error jobs
        (
            pipeline_module.schema.jobs
            & 'status = "error"'
            & [f'error_message LIKE "{e}"' for e in _generic_errors]
        ).delete()
        # clear additional error patterns
        additional_error_query = (
            pipeline_module.schema.jobs
            & 'status = "error"'
            & [f'error_message LIKE "{e}"' for e in additional_error_patterns]
        )

        with pipeline_module.schema.jobs.connection.transaction:
            for error_entry in additional_error_query.fetch(as_dict=True):
                ErrorLog.log_error_job(
                    error_entry,
                    schema_name=pipeline_module.schema.database,
                    db_prefix=db_prefix,
                )
            additional_error_query.delete()

        # Handle stale reserved jobs
        handle_stale_reserved_jobs(pipeline_module, stale_timeout_hours, action="error")


def purge_invalid_jobs(JobTable, table):
    """
    Check and remove any invalid/outdated jobs in the JobTable for this autopopulate table
    Job keys that are in the JobTable (regardless of status) but
    - are no longer in the `key_source`
        (e.g. jobs added but entries in upstream table(s) got deleted)
    - are present in the "target" table
        (e.g. completed by another process/user)
    This is potentially a time-consuming process - but should not expect to have to run very often
    """

    jobs_query = JobTable & {"table_name": table.table_name}

    if not jobs_query:
        return

    invalid_removed = 0
    for key, job_key in zip(*jobs_query.fetch("KEY", "key")):
        if (not (table.key_source & job_key)) or (table & job_key):
            (jobs_query & key).delete()
            invalid_removed += 1

    logger.info(
        f"\t{invalid_removed} invalid jobs removed for `{dj.utils.to_camel_case(table.table_name)}`"
    )


def handle_stale_reserved_jobs(
    pipeline_module,
    stale_timeout_hours: int = 24,
    action: str = "error",
):
    """
    Handle stale reserved jobs by either marking them as errors or removing them.
    A job is considered stale if it meets BOTH conditions:
    1. Has been in "reserved" status for longer than the specified time limit (in hours)
    2. The connection_id associated with the job is no longer active

    Args:
        pipeline_module: The pipeline module containing the jobs table
        stale_timeout_hours: Time limit in hours after which a reserved job is considered stale
        action: What to do with stale jobs:
            - "error" (default): Mark stale jobs as errors
            - "remove": Remove stale jobs
            - None: Return the stale jobs table without modifying them

    Returns:
        None if action is "error" or "remove"
        dj.Table containing stale jobs if action is None
    """
    if stale_timeout_hours <= 0:
        return None if action is not None else pipeline_module.schema.jobs & "false"

    stale_jobs = (
        pipeline_module.schema.jobs.proj(
            ..., elapsed_hours="TIMESTAMPDIFF(HOUR, timestamp, NOW())"
        )
        & 'status = "reserved"'
        & f"elapsed_hours > {stale_timeout_hours}"
    )

    current_connections = [
        v[0]
        for v in pipeline_module.schema.connection.query(
            "SELECT id FROM information_schema.processlist WHERE id <> CONNECTION_ID() ORDER BY id"
        )
    ]

    if current_connections:
        current_connections = f'({", ".join([str(c) for c in current_connections])})'
        stale_jobs &= f"connection_id NOT IN {current_connections}"

    if action is None:
        return stale_jobs
    elif action == "remove":
        (pipeline_module.schema.jobs & stale_jobs.fetch("KEY")).delete()
    elif action == "error":
        schema_name = pipeline_module.schema.database
        error_message = (
            "Stale reserved job (process crashed or terminated without error)"
        )
        for table_name, job_key in zip(*stale_jobs.fetch("table_name", "key")):
            pipeline_module.schema.jobs.error(
                table_name, job_key, error_message=error_message
            )
            full_table_name = f"`{schema_name}`.`{table_name}`"
            logger.debug(
                f"Error making {job_key} -> {full_table_name} - {error_message}"
            )
    else:
        raise ValueError(
            f"Invalid action: {action}, must be 'error', 'remove', or None"
        )
