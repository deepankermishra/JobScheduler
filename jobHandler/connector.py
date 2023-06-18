import time
import random
from .handler import Handler

class ConnectorHandler(Handler):

  def executeTask(self):
    print("Connector for jobId:", self.job.jobId)
    if random.randint(0, 1) == 0:
      # randomly sleep for longer to induce job timeout
      time.sleep(10)
    # Fail connector jobs
    raise Exception("Random exception occurred")
    return True