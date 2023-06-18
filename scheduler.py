"""
Scheduler goes over all jobs at a regular interval. It schedules jobs that are ready,
aborts jobs that have timedout and puts job to waiting if their waiting window is not
over.
"""

from datetime import datetime, timedelta
from job import JobStatus


def shouldSchedule(job):
  if (job.status not in [JobStatus.READY]):
    return False
  return True


def scheduleJob(job):
  if shouldSchedule(job):
    print("Job scheduled:", job.jobId)
    job.status = JobStatus.RUNNING
    job.startedAt = datetime.utcnow()
    # Should be dispatched to another environment, for example, it can be an AWS Lambda job.
    job.run()
  elif job.status == JobStatus.RUNNING and (datetime.utcnow() - job.updatedAt > job.timeoutInSec):
    # Job is not making progress and should be aborted with error.
    job.status = JobStatus.ERROR
  elif job.status in [JobStatus.SUCCESS, JobStatus.WAITING, JobStatus.ERROR]:
    if job.shouldWait():
      job.status = JobStatus.WAITING
    else:
      job.status = JobStatus.READY
  else:
    print("Job not ready for scheduling", job.jobId)
    return
  print("Marking job:", job.jobId, "as", job.status)


def scheduleJobs(jobs):
  for job in jobs:
    scheduleJob(job)
