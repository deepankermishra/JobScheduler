import uuid
import csv
from datetime import datetime, timezone
from pathlib import Path


from job import Job, JobStatus, JobType

# Maintaining all jobs in memory and writing to csv for simplicity. These should be maintained in a database.
JOBS_TABLE_FILES_DIR = './data/'
JOBS_TABLE_FILE_NAME = 'jobsTableIn.csv'
JOBS_TABLE_FILE_NAME = 'jobsTableOut.csv'
JOBS_TABLE_FIELDNAMES = []


def stringToEnum(value):
	if type(value) == str:
		return eval(value)
	return value

def getJobObjFromDict(jobDict):
  for key in jobDict:
    if key == 'jobId':
      jobDict[key] = uuid.UUID(jobDict[key])
    elif 'At' in key and jobDict[key]:
      jobDict[key] = datetime.strptime(jobDict[key], "%Y-%m-%d %H:%M:%S.%f")
      # jobDict[key] = datetime.fromisoformat(jobDict[key])
    elif jobDict[key]:
      jobDict[key] = eval(jobDict[key])
  jobObj = Job(**jobDict)
  jobObj.jobType = stringToEnum(jobObj.jobType)
  jobObj.status = stringToEnum(jobObj.status)
  return jobObj

# Generate jobs and write it to an input csv file.
def jobDataInputGenerator(numRowsToGenerate = 5):
  Path(JOBS_TABLE_FILES_DIR).mkdir(parents=True, exist_ok=True)
  with open(JOBS_TABLE_FILES_DIR + JOBS_TABLE_FILE_NAME, 'w', newline='') as file:
    try:
      writer = csv.writer(file)
      # write header
      writer.writerow(["jobId","createdAt","updatedAt","startedAt","endedAt","status","jobType","errorCode","errorMessage","errorCount","waitWindowInSec","timeoutInSec"])
      for _ in range(numRowsToGenerate):
        job = Job(JobType.CONNECTOR)  
        writer.writerow([job.jobId, job.createdAt, job.updatedAt, job.startedAt, job.endedAt, job.status, job.jobType, job.errorCode, job.errorMessage, job.errorCount, job.waitWindowInSec, job.timeoutInSec])
      for _ in range(numRowsToGenerate):
        job = Job(JobType.TRANSFORMER)
        writer.writerow([job.jobId, job.createdAt, job.updatedAt, job.startedAt, job.endedAt, job.status, job.jobType, job.errorCode, job.errorMessage, job.errorCount, job.waitWindowInSec, job.timeoutInSec])
    except BaseException as e:
        print('BaseException:', e, JOBS_TABLE_FILE_NAME)
    else:
        print('Data written successfully')

# Loads data from CSV to in-memory array.
# This can be updated to use a DB query.
def jobDataLoader():
  Path(JOBS_TABLE_FILES_DIR).mkdir(parents=True, exist_ok=True)
  global JOBS_TABLE_FIELDNAMES
  ALL_JOBS_IN_MEMORY = []

  with open(JOBS_TABLE_FILES_DIR + JOBS_TABLE_FILE_NAME, 'r') as csvFile:
    jobsTableContent = csv.DictReader(csvFile)
    JOBS_TABLE_FIELDNAMES = jobsTableContent.fieldnames
    headerRow = True
    for jobDict in jobsTableContent:
      if headerRow:
        headerRow = False
        continue
      ALL_JOBS_IN_MEMORY.append(getJobObjFromDict(jobDict))
  return ALL_JOBS_IN_MEMORY


# Writes the in-memory job array contents to a CSV file.
def jobDataDumper(scheduledJobs, instant):
  Path(JOBS_TABLE_FILES_DIR).mkdir(parents=True, exist_ok=True)
  global JOBS_TABLE_FIELDNAMES
  outFilePath = JOBS_TABLE_FILE_NAME
  if instant:
    outFileName, outFileExtension = outFilePath.split(".")
    outFilePath = outFileName + str(instant) + outFileExtension
  with open(JOBS_TABLE_FILES_DIR + outFilePath, 'w') as csvFile:
    writer = csv.DictWriter(csvFile, fieldnames=JOBS_TABLE_FIELDNAMES)
    writer.writeheader()
    for job in scheduledJobs:
      writer.writerow(job.__dict__)
