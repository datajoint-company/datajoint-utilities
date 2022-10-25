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

import argparse
import inspect
import json
import logging
import os
import platform
import re
import time
import traceback
from datetime import datetime

import datajoint as dj
import pymysql
from datajoint.user_tables import Part, UserTable


def is_djtable(obj, base_class=None) -> bool:
    if base_class is None:
        base_class = UserTable
    return isinstance(obj, base_class) or (
        inspect.isclass(obj) and issubclass(obj, base_class)
    )


def is_djparttable(obj) -> bool:
    return is_djtable(obj, Part)


logger = dj.logger

_populate_settings = {
    "display_progress": True,
    "reserve_jobs": True,
    "suppress_errors": True,
}


class WorkerLog(dj.Manual):
    definition = """
    # Registration of processing jobs running .populate() jobs or custom function
    process_timestamp : datetime(6)   # timestamp of the processing job (UTC)
    process           : varchar(64)
    ---
    worker_name=''    : varchar(255)  # name of the worker
    host              : varchar(255)  # system hostname
    user=''           : varchar(255)  # database user
    pid=0             : int unsigned  # system process id
    """

    _table_name = "~worker_log"

    @classmethod
    def log_process_job(cls, process, worker_name="", db_prefix=("",)):
        if is_djtable(process):
            schema_name, table_name = process.full_table_name.split(".")
            schema_name = re.sub("|".join(db_prefix), "", schema_name.strip("`"))
            table_name = dj.utils.to_camel_case(table_name.strip("`"))
            process_name = f"{schema_name}.{table_name}"
            user = process.connection.get_user()
        elif inspect.isfunction(process) or inspect.ismethod(process):
            process_name = process.__name__
            user = ""
        else:
            raise ValueError(
                "Input process must be either a DataJoint table or a function"
            )

        if not worker_name:
            frame = inspect.currentframe()
            function_name = frame.f_back.f_code.co_name
            module_name = inspect.getmodule(frame.f_back).__name__
            worker_name = f"{module_name}.{function_name}"

        cls.insert1(
            {
                "process": process_name,
                "process_timestamp": datetime.utcnow(),
                "worker_name": worker_name,
                "host": platform.node(),
                "user": user,
                "pid": os.getpid(),
            }
        )

    @classmethod
    def get_recent_jobs(cls, backtrack_minutes=60):
        recent = (
            cls.proj(
                minute_elapsed="TIMESTAMPDIFF(MINUTE, process_timestamp, UTC_TIMESTAMP())"
            )
            & f"minute_elapsed < {backtrack_minutes}"
        )

        recent_jobs = dj.U("process").aggr(
            cls & recent,
            worker_count="count(DISTINCT pid)",
            minutes_since_oldest="TIMESTAMPDIFF(MINUTE, MIN(process_timestamp), UTC_TIMESTAMP())",
            minutes_since_newest="TIMESTAMPDIFF(MINUTE, MAX(process_timestamp), UTC_TIMESTAMP())",
        )

        return recent_jobs

    @classmethod
    def delete_old_logs(cls, cutoff_days=30):
        old_jobs = (
            cls.proj(
                elapsed_days=f'TIMESTAMPDIFF(DAY, process_timestamp, "{datetime.utcnow()}")'
            )
            & f"elapsed_days > {cutoff_days}"
        )
        if old_jobs:
            with dj.config(safemode=False):
                try:
                    (cls & old_jobs).delete_quick()
                except pymysql.err.OperationalError:
                    pass


class ErrorLog(dj.Manual):
    definition = """
    # Logging of job errors
    process           : varchar(64)
    key_hash          : char(32)      # key hash
    ---
    error_timestamp   : datetime(6)   # timestamp of the processing job (UTC)
    key               : varchar(2047) # structure containing the key
    error_message=""  : varchar(2047) # error message returned if failed
    error_stack=null  : mediumblob    # error stack if failed
    host              : varchar(255)  # system hostname
    user=''           : varchar(255)  # database user
    pid=0             : int unsigned  # system process id
    """

    _table_name = "~error_log"

    @classmethod
    def log_error_job(cls, error_entry, schema_name, db_prefix=("",)):
        # if the exact same error has been logged, just update the error record

        table_name = error_entry["table_name"]
        schema_name = re.sub("|".join(db_prefix), "", schema_name.strip("`"))
        table_name = dj.utils.to_camel_case(table_name.strip("`"))
        process_name = f"{schema_name}.{table_name}"

        entry = {
            "process": process_name,
            "key_hash": error_entry["key_hash"],
            "error_timestamp": error_entry["timestamp"],
            "key": json.dumps(error_entry["key"], default=str),
            "error_message": error_entry["error_message"],
            "error_stack": error_entry["error_stack"],
            "host": error_entry["host"],
            "user": error_entry["user"],
            "pid": error_entry["pid"],
        }

        if cls & {"process": entry["process"], "key_hash": entry["key_hash"]}:
            cls.update1(entry)
        else:
            cls.insert1(entry)

    @classmethod
    def log_exception(cls, key, process, error):
        error_message = "{exception}{msg}".format(
            exception=error.__class__.__name__,
            msg=": " + str(error) if str(error) else "",
        )
        entry = {
            "process": process.__name__,
            "key_hash": dj.hash.key_hash(key),
            "error_timestamp": datetime.utcnow(),
            "key": json.dumps(key, default=str),
            "error_message": error_message,
            "error_stack": traceback.format_exc(),
            "host": platform.node(),
            "user": cls.connection.get_user(),
            "pid": os.getpid(),
        }

        if cls & {"process": entry["process"], "key_hash": entry["key_hash"]}:
            cls.update1(entry)
        else:
            cls.insert1(entry)

    @classmethod
    def delete_old_logs(cls, cutoff_days=30):
        old_jobs = (
            cls.proj(
                elapsed_days=f'TIMESTAMPDIFF(DAY, error_timestamp, "{datetime.utcnow()}")'
            )
            & f"elapsed_days > {cutoff_days}"
        )
        if old_jobs:
            with dj.config(safemode=False):
                try:
                    (cls & old_jobs).delete_quick()
                except pymysql.err.OperationalError:
                    pass


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
        autoclear_error_patterns=[],
        db_prefix=[""],
    ):
        self.name = worker_name
        self._worker_schema = dj.schema(worker_schema_name)
        self._worker_schema(WorkerLog)
        self._worker_schema(ErrorLog)

        self._autoclear_error_patterns = autoclear_error_patterns
        self._run_duration = run_duration
        self._sleep_duration = sleep_duration
        self._db_prefix = [db_prefix] if isinstance(db_prefix, str) else db_prefix

        self._processes_to_run = []
        self._pipeline_modules = {}

    def __call__(self, process, **kwargs):
        self.add_step(process, **kwargs)

    def add_step(self, callable, position_=None, **kwargs):
        index = len(self._processes_to_run) if position_ is None else position_
        if is_djtable(callable):
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

    def _run_once(self):
        for process_type, process, kwargs in self._processes_to_run:
            WorkerLog.log_process_job(
                process, worker_name=self.name, db_prefix=self._db_prefix
            )
            if process_type == "dj_table":
                process.populate(**{**_populate_settings, **kwargs})
            elif process_type == "function":
                process(**kwargs)

        _clean_up(
            self._pipeline_modules.values(),
            additional_error_patterns=self._autoclear_error_patterns,
            db_prefix=self._db_prefix,
        )

        WorkerLog.delete_old_logs()
        ErrorLog.delete_old_logs()

    def run(self):
        start_time = time.time()
        while (
            time.time() - start_time < self._run_duration
            or self._run_duration is None
            or self._run_duration < 0
        ):
            try:
                self._run_once()
            except Exception as e:
                logger.error(str(e))

            time.sleep(self._sleep_duration)


def _clean_up(pipeline_modules, additional_error_patterns=[], db_prefix=""):
    """
    Routine to clear entries from the jobs table that are:
    + generic-type error jobs
    + stale "reserved" jobs
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


# arg-parser for usage as CLI

# combine different formatters
class ArgumentDefaultsRawDescriptionHelpFormatter(
    argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter
):
    pass


def parse_args(args):
    """
    Parse command line parameters
    :param args: command line parameters as list of strings (for example  ``["--help"]``)
    :type args: List[str]
    :return: `argparse.Namespace`: command line parameters namespace
    :rtype: obj
    """

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=ArgumentDefaultsRawDescriptionHelpFormatter
    )

    parser.add_argument("worker_name", help="Select the worker to run", type=str)

    parser.add_argument(
        "-d",
        "--duration",
        dest="duration",
        help="Run duration of the entire process",
        type=int,
        metavar="INT",
        default=None,
    )

    parser.add_argument(
        "-s",
        "--sleep",
        dest="sleep",
        help="Sleep time between subsequent runs",
        type=int,
        metavar="INT",
        default=None,
    )

    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="Set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )

    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="Set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )

    return parser.parse_args(args)
