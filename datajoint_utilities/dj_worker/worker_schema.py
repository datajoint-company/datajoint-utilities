import json
import os
import platform
import traceback
import pymysql
import inspect
import re
from datetime import datetime
import datajoint as dj
import numpy as np
import pandas as pd

from datajoint.user_tables import Part, UserTable


class RegisteredWorker(dj.Manual):
    definition = """
    worker_name: varchar(64)
    ---
    registration_time: datetime(6) # registration datetime (UTC)
    worker_kwargs: longblob  # keyword arguments to instantiate a DataJointWorker class
    worker_config_uuid: uuid
    unique index (worker_name, worker_config_uuid)
    """

    class Process(dj.Part):
        definition = """
        -> master
        process: varchar(64)
        ---
        process_index: int  # running order for this process from this worker
        process_config_uuid: UUID # hash of the key_source
        full_table_name='': varchar(255)  # full table name if this process is a DJ table
        key_source_sql=null  :longblob  # sql statement for the key_source of the table - from make_sql()
        process_kwargs: longblob  # keyword arguments to pass when calling the process
        unique index (worker_name, process, process_config_uuid)
        """

    @classmethod
    def get_workers_progress(cls):
        """
        Return the operation progress for all registered workers (jobs status for each process)
        :return: pandas DataFrame of workers jobs status
        """
        workflow_status = (
            cls.Process.proj(
                "full_table_name", "key_source_sql", total="NULL", in_queue="NULL"
            )
            .fetch(format="frame")
            .reset_index()
        )

        schema_names = set(
            [n.split(".")[0].strip("`") for n in workflow_status.full_table_name if n]
        )
        pipeline_schemas = {n: dj.Schema(n) for n in schema_names}
        if not pipeline_schemas:
            return pd.DataFrame()

        job_status_df = {"reserved": [], "error": [], "ignore": []}
        for pipeline_schema in pipeline_schemas.values():
            for job_status in ("reserved", "error", "ignore"):
                status_df = (
                    dj.U("table_name")
                    .aggr(
                        pipeline_schema.jobs & f'status = "{job_status}"',
                        **{job_status: "count(table_name)"},
                    )
                    .fetch(format="frame")
                )
                status_df.index = status_df.index.map(
                    lambda x: f"{pipeline_schema.database}.{x}"
                )
                job_status_df[job_status].append(status_df)

        for k, v in job_status_df.items():
            job_status_df[k] = pd.concat(v)

        _total, _in_queue = [], []
        for _, r in workflow_status.iterrows():
            if not r.full_table_name:
                continue
            target = dj.FreeTable(full_table_name=r.full_table_name, conn=dj.conn())
            r.total, r.in_queue = cls._get_key_source_count(r.key_source_sql, target)

        workflow_status = workflow_status.join(
            job_status_df["reserved"]
            .join(job_status_df["error"], how="outer")
            .join(job_status_df["ignore"], how="outer"),
            how="left",
        )

        workflow_status.fillna(0, inplace=True)
        workflow_status.replace(np.inf, np.nan, inplace=True)

        workflow_status["remaining"] = (
            workflow_status.in_queue
            - workflow_status.reserved
            - workflow_status.error
            - workflow_status.ignore
        )

        workflow_status.drop(
            columns=["full_table_name", "key_source_sql"], inplace=True
        )
        return workflow_status

    @classmethod
    def _get_key_source_count(cls, key_source_sql, target):
        def _rename_attributes(table, props):
            return (
                table.proj(
                    **{
                        attr: ref
                        for attr, ref in props["attr_map"].items()
                        if attr != ref
                    }
                )
                if props["aliased"]
                else table.proj()
            )

        parents = target.parents(primary=True, as_objects=True, foreign_key_info=True)

        ks_tbl = _rename_attributes(*parents[0])
        for q in parents[1:]:
            ks_tbl *= _rename_attributes(*q)

        total = len(dj.conn().query(key_source_sql).fetchall())
        in_queue_sql = (
            key_source_sql
            + f"{'AND' if 'WHERE' in key_source_sql else 'WHERE'}(({ks_tbl.heading.as_sql(ks_tbl.heading.primary_key)}) not in ({ks_tbl.proj().make_sql()}))"
        )
        in_queue = len(dj.conn().query(in_queue_sql).fetchall())
        return total, in_queue


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
        process_name = get_process_name(db_prefix, process)
        user = dj.conn().get_user()

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
        # if latest log is older than cutoff_days, then do nothing
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


def get_process_name(process, db_prefix):
    if is_djtable(process):
        schema_name, table_name = process.full_table_name.split(".")
        schema_name = re.sub("|".join(db_prefix), "", schema_name.strip("`"))
        table_name = dj.utils.to_camel_case(table_name.strip("`"))
        process_name = f"{schema_name}.{table_name}"
    elif inspect.isfunction(process) or inspect.ismethod(process):
        process_name = process.__name__
    else:
        raise ValueError("Input process must be either a DataJoint table or a function")
    return process_name


def is_djtable(obj, base_class=None) -> bool:
    if base_class is None:
        base_class = UserTable
    return isinstance(obj, base_class) or (
        inspect.isclass(obj) and issubclass(obj, base_class)
    )


def is_djparttable(obj) -> bool:
    return is_djtable(obj, Part)
