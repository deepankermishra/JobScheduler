import time

from utils import jobDataLoader, jobDataDumper, jobDataInputGenerator
from scheduler import scheduleJobs


RUN_SCHEDULER_TIMES = 5

def main():
    # Generate test data as a csv table
    jobDataInputGenerator()

    # Get table contents from csv
    allJobsInMemory = jobDataLoader()

    # Schedule jobs
    for inst in range(RUN_SCHEDULER_TIMES):
      print("Scheduler started")
      scheduleJobs(allJobsInMemory)
      # Update table contents
      jobDataDumper(allJobsInMemory, inst)
      
      print("Scheduler sleeping")
      time.sleep(2)


if __name__ == '__main__':
    main()
