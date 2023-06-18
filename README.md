# Job Scheduler

To run use:
```
python3 main.py
```

`NOTE: This scheduler uses CSV files for storing data across scheduler runs. This data should be maintained in a DB.`

## Job Scheduler:

1. The job scheduler goes over all tasks at a regular interval and keeps scheduling jobs that are READY.
2. The job-scheduler checks if a particular run is making progress by checking its updatedAt
timestamp. If a job does not get updated within a given time window, it is aborted and the status is changed
to ERROR. 


## Job:
Each job has a job-type that indicates the tasks that it's supposed to run and has a job-status which is the
state of the job. The Job class also maintains information about start time, end time and other important
parameters of a job.


## Job Handler:
These are tasks that a job is responsible for executing.