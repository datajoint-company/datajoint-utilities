# dj-worker

Mechanism to define ***worker(s)*** to operate a DataJoint pipeline.

The workers orchestrate running the populate routines with logging for 
better progress and status monitoring


# Usage


## Initialize some workers to operate your pipeline

```
from datajoint_utilities.dj_worker import DataJointWorker, WorkerLog

db_prefix = 'my_pipeline_'

worker_schema_name = db_prefix + 'workerlog'

worker1 = DataJointWorker('worker1',
                         worker_schema_name,
                         db_prefix=db_prefix,
                         run_duration=3600*3,
                         sleep_duration=10)

worker2 = DataJointWorker('worker2',
                         worker_schema_name,
                         db_prefix=db_prefix,
                         run_duration=-1,
                         sleep_duration=10)
```

## Decorate DJ tables to be operated by each worker


```

@schema
@worker1
class AnalysisTable(dj.Computed):
    definition = """
    ...
    """
    
    def make(self, key):
        # some analysis code
        pass    


@schema
@worker2
class AnalysisTable2(dj.Computed):
    definition = """
    ...
    """
    
    def make(self, key):
        # some analysis code
        pass  
```


## Run the workers

```
worker1.run()
worker2.run()
```


## Monitor worker status

```
WorkerLog.print_recent_jobs()
```

## Retrieve workers, jobs status and progress

```
from datajoint_utilities.dj_worker.utils import get_workflow_operation_overview

worker_schema_name = 'my_pipeline_workerlog'

workflow_overview = get_workflow_operation_overview(worker_schema_name)
```