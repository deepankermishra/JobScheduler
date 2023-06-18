import uuid

from enum import Enum
from datetime import datetime, timedelta

from err import Error
from jobHandler.transformer import TransformerHandler
from jobHandler.connector import ConnectorHandler


# If job is not sending updates it should be aborted
DEFAULT_TIMEOUT_IN_SEC = 6
# Wait for 5 secs before scheduling
DEFAULT_WAIT_WINDOW_IN_SEC = 2
# Number of errors can a job see before transitioning to FAILED
ERROR_THRESHOLD = 2


class JobType(Enum):
  CONNECTOR = 1
  TRANSFORMER = 2


class JobStatus(Enum):
  WAITING = 0
  READY = 1
  RUNNING = 2
  SUCCESS = 3
  FAILED = 4
  ERROR = 5


class Job:
  """
  Job class maintains all information for a work unit. The work is defined by a handler which the job class is oblivious of.
  """
  def __init__(self, jobType, jobId=None, createdAt=None, updatedAt=None, startedAt=None, endedAt=None, status=None, errorCode=None, errorMessage=None, errorCount=None, waitWindowInSec=DEFAULT_WAIT_WINDOW_IN_SEC, timeoutInSec=DEFAULT_TIMEOUT_IN_SEC):
    if jobId is None:
      self.jobId = uuid.uuid4()
    else:
      self.jobId = jobId
    self.createdAt = createdAt or datetime.utcnow()
    self.updatedAt = updatedAt or self.createdAt
    self.status = status or JobStatus.READY
    self.jobType = jobType
    self.waitWindowInSec = waitWindowInSec
    self.timeoutInSec = timeoutInSec
    self.errorCount = errorCount or 0
    self.errorCode = errorCode
    self.errorMessage = errorMessage
    self.startedAt = startedAt
    self.endedAt = endedAt

  def resetErrorAttributes(self):
    self.errorCode = None
    self.errorMessage = None
    self.errorCount = 0

  def errorHandler(self, exp):
    return Error()

  # A simple wait condition can change with job and type
  def shouldWait(self):
    if self.endedAt and datetime.utcnow() < self.endedAt + timedelta(seconds=self.waitWindowInSec):
      return True
    return False

  def nextJobStatus(self):
    currentJobStatus = self.status
    if currentJobStatus == JobStatus.RUNNING:
      self.status = JobStatus.SUCCESS
    if currentJobStatus == JobStatus.ERROR and self.errorCount > ERROR_THRESHOLD:
      self.status = JobStatus.FAILED

  def run(self):
    handler = handlerFactory(self)
    done = False
    while(self.status == JobStatus.RUNNING and not done):
      print("Running job:", self.jobId)
      try:
        done = handler.executeTask()
      except Exception as exp:
        print("Exception has occurred:", exp)
        # Error code and message should be provided by the error handler depending on the job,
        # as it will be specific to each job.
        handledError = self.errorHandler(exp)
        self.errorCode = handledError.errorCode
        self.errorMessage = handledError.errorMessage
        self.errorCount += 1
        self.status = JobStatus.ERROR
        # Exponential backoff in case of error.
        self.waitWindowInSec *= 2
        done = True
      finally:
        if(done):
          self.endedAt = datetime.utcnow()
          self.nextJobStatus()
        self.updatedAt = datetime.utcnow()


"""
Choose the work depending upon job type.
"""
def handlerFactory(job):
  jobType = job.jobType
  if jobType == JobType.TRANSFORMER:
    return TransformerHandler(job)
  elif jobType == JobType.CONNECTOR:
    return ConnectorHandler(job)
  else:
    raise ValueError("Invalid job type: {}".format(jobType))