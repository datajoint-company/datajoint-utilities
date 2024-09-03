"""
Mechanism to set up and manage "workers" to operate a DataJoint pipeline
Each "worker" is run in a while-loop with the total run-duration configurable via
command line argument '--duration' (if not set, runs perpetually)
    - the loop will not begin a new cycle after this period of time (in seconds)
    - the loop will run perpetually if duration<0 or if duration==None
    - the script will not be killed _at_ this limit, it will keep executing,
      and just stop repeating after the time limit is exceeded
Some populate settings (e.g. 'limit', 'max_calls') can be set to process some number of jobs at
a time for every iteration of the loop, instead of all jobs. This allows for the controll of the processing to
propagate through the pipeline more horizontally or vertically.
"""
import inspect
import time
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
    A decorator class for running and managing the populate jobs
    """

    def __init__(
        self,
        worker_name,
        worker_schema_name,
        *,
        run_duration=-1,
        sleep_duration=60,
        max_idled_cycle=-1,
        autoclear_error_patterns=[],
        db_prefix=[""],
        remove_stale_reserved_jobs=True,
    ):
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
        self._remove_stale_reserved_jobs = remove_stale_reserved_jobs

        self._processes_to_run = []
        self._pipeline_modules = {}
        self._idled_cycle_count = None
        self._run_start_time = None
        self._is_registered = False

    def __call__(self, process, **kwargs):
        self.add_step(process, **kwargs)

    def add_step(self, callable, position_=None, **kwargs):
        """
        Add a new process to the list of processes to be executed by this worker
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

    def register_worker(self):
        """
        Register the worker and its associated processes into the RegisteredWorker table
        """
        if self._is_registered:
            return

        process_entries = []
        for index, (process_type, process, kwargs) in enumerate(self._processes_to_run):
            entry = {
                "worker_name": self.name,
                "process": get_process_name(process, self._db_prefix),
                "process_index": index,
                "full_table_name": process.full_table_name
                if process_type == "dj_table"
                else "",
                "key_source_sql": process.key_source.proj().make_sql()
                if process_type == "dj_table"
                else None,
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

    def _run_once(self):
        """
        Run all processes in order, once
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
            remove_stale_reserved_jobs=self._remove_stale_reserved_jobs,
        )

        WorkerLog.delete_old_logs()
        ErrorLog.delete_old_logs()

        return success_count

    def _keep_running(self):
        """
        Determine whether or not to keep running all the processes again and again
        """
        exceed_run_duration = not (
            time.time() - self._run_start_time < self._run_duration
            or self._run_duration < 0
        )
        exceed_max_idled_cycle = 0 < self._max_idled_cycle < self._idled_cycle_count
        return not (exceed_run_duration or exceed_max_idled_cycle)

    def _purge_invalid_jobs(self):
        for process_type, process, _ in self._processes_to_run:
            if process_type == "dj_table":
                vmod = self._pipeline_modules[process.database]
                purge_invalid_jobs(vmod.schema.jobs, process)

    def run(self):
        """
        Run all processes in a continual loop until the terminating condition is met (see "_keep_running()")
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


def _clean_up(pipeline_modules, additional_error_patterns=[], db_prefix="", remove_stale_reserved_jobs=True):
    """
    Routine to clear entries from the jobs table that are:
    + generic-type error jobs
    + stale "reserved" jobs
        Stale "reserved" jobs are jobs with "reserved" status but the connection_id is not in the list of current connections
        This could be due to a hard crash of the worker process or a network issue
        However, there could be "reserved" jobs without a connection_id, but is actually being processed
        Thus, identifying "stale reserved jobs" is not a guaranteed process, and should be used with caution
        This ambiguity will be solved in future DataJoint version
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

        # clear stale "reserved" jobs
        if remove_stale_reserved_jobs:
            current_connections = [
                v[0]
                for v in dj.conn().query(
                    "SELECT id FROM information_schema.processlist WHERE id <> CONNECTION_ID() ORDER BY id"
                )
            ]
            if current_connections:
                current_connections = (
                    f'({", ".join([str(c) for c in current_connections])})'
                )
                stale_jobs = (
                    pipeline_module.schema.jobs
                    & 'status = "reserved"'
                    & f"connection_id NOT IN {current_connections}"
                )
                (pipeline_module.schema.jobs & stale_jobs.fetch("KEY")).delete()


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
