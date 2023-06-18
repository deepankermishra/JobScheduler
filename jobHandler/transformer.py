import time
import random
from .handler import Handler

class TransformerHandler(Handler):

  def executeTask(self):
    print("Transformer for job:", self.job.jobId)
    time.sleep(1)
    # Randomly raise an exception
    if random.randint(0, 1) == 0:
      raise Exception("Random exception occurred")
    return True
