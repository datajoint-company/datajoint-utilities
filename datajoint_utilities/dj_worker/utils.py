import os
import datajoint as dj
import pandas as pd
import numpy as np
import re

from .worker_schema import RegisteredWorker


os.environ["DJ_SUPPORT_FILEPATH_MANAGEMENT"] = "TRUE"
logger = dj.logger


def _get_workflow_progress(db_prefixes, exclude_tables=()):
    """
    Function to retrieve the processing status of a workflow comprised of all schemas with the specified prefix(es)
    Return a dataframe with all Imported/Computed tables, and their corresponding processing status:
        + total
        + incomplete
        + reserved
        + error
        + ignore
        + remaining

    Example usage:
        db_prefixes = ['my_project_']
        workflow_status = get_workflow_progress(db_prefixes)

    (Note: not topologically sorted)
    """
    pipeline_modules = {
        n: dj.create_virtual_module(n, n)
        for n in dj.list_schemas()
        if re.match("|".join(db_prefixes), n)
    }
    if not pipeline_modules:
        return pd.DataFrame()

    job_status_df = {"reserved": [], "error": [], "ignore": []}
    for pipeline_module in pipeline_modules.values():
        for job_status in ("reserved", "error", "ignore"):
            status_df = (
                dj.U("table_name")
                .aggr(
                    pipeline_module.schema.jobs & f'status = "{job_status}"',
                    **{job_status: "count(table_name)"},
                )
                .fetch(format="frame")
            )
            status_df.index = status_df.index.map(
                lambda x: f"{pipeline_module.schema.database}.{x}"
            )
            job_status_df[job_status].append(status_df)

    for k, v in job_status_df.items():
        job_status_df[k] = pd.concat(v)

    process_tables = {}
    for pipeline_module in pipeline_modules.values():
        module_tables = {}
        pipeline_module.schema.spawn_missing_classes(context=module_tables)
        process_tables = {
            **process_tables,
            **{
                process.full_table_name.replace("`", ""): process
                for process in module_tables.values()
                if process.table_name.startswith("_")
            },
        }

    workflow_status = pd.DataFrame(list(process_tables), columns=["table_name"])
    workflow_status.set_index("table_name", inplace=True)

    _total, _incomplete = [], []
    for t in workflow_status.index:
        logger.debug(f"\tprocessing {t}...")
        _total.append(
            np.inf
            if process_tables[t].__name__ in exclude_tables
            else len(process_tables[t].key_source)
        )
        _incomplete.append(
            np.inf
            if process_tables[t].__name__ in exclude_tables
            else len(process_tables[t].key_source - process_tables[t].proj())
        )

    workflow_status["total"] = _total
    workflow_status["incomplete"] = _incomplete

    workflow_status = workflow_status.join(
        job_status_df["reserved"]
        .join(job_status_df["error"], how="outer")
        .join(job_status_df["ignore"], how="outer"),
        how="left",
    )

    workflow_status.fillna(0, inplace=True)
    workflow_status.replace(np.inf, np.nan, inplace=True)

    workflow_status["remaining"] = (
        workflow_status.incomplete
        - workflow_status.reserved
        - workflow_status.error
        - workflow_status.ignore
    )

    def _format_table_name(full_table_name):
        schema_name, table_name = full_table_name.split(".")
        schema_name = re.sub("|".join(db_prefixes), "", schema_name)
        table_name = dj.utils.to_camel_case(table_name)
        return f"{schema_name}.{table_name}"

    workflow_status.index = workflow_status.index.map(_format_table_name)

    return workflow_status


def get_workflow_operation_overview(
    worker_schema_name, db_prefixes=None, exclude_tables=()
):
    try:
        workerlog_vm = dj.create_virtual_module("worker_vm", worker_schema_name)
    except dj.errors.DataJointError:
        return pd.DataFrame()

    # -- New method to retrieve workflow_operation_overview more accurately, accounting for modified key_source
    if hasattr(workerlog_vm, RegisteredWorker.__name__):
        _schema = dj.schema(worker_schema_name)
        _schema(RegisteredWorker)
        # confirm workers' processes overlap with the specified db_prefixes
        is_valid_db_prefixes = db_prefixes is None or set(
            [
                n.split(".")[0].strip("`")
                for n in (
                    RegisteredWorker.Process & "key_source_sql is not NULL"
                ).fetch("full_table_name")
                if re.match("|".join(db_prefixes), n.split(".")[0].strip("`"))
            ]
        )
        return (
            RegisteredWorker.get_workers_progress()
            if is_valid_db_prefixes
            else pd.DataFrame()
        )

    # -- Old method to retrieve workflow_operation_overview - modified key_source unaccounted for
    if db_prefixes is None:
        raise ValueError(f"db_prefixes must be specified")

    # workflow_progress
    workflow_progress = _get_workflow_progress(
        db_prefixes, exclude_tables=exclude_tables
    )
    if workflow_progress.empty:
        return pd.DataFrame()
    # workers
    worker_mapping = (
        dj.U("worker_name")
        .aggr(
            workerlog_vm.WorkerLog,
            table_name='GROUP_CONCAT(DISTINCT(process) SEPARATOR ",")',
        )
        .fetch(format="frame")
        .reset_index()
    )
    worker_mapping.table_name = worker_mapping.table_name.apply(
        lambda x: [p for p in x.split(",") if p in workflow_progress.index]
    )
    worker_mapping = worker_mapping.explode("table_name")
    worker_mapping.set_index("table_name", inplace=True)

    workflow_overview = workflow_progress.join(worker_mapping).applymap(
        lambda x: x if not isinstance(x, int) or x > 0 else 0
    )
    return workflow_overview
