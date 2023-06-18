class Handler:
  def __init__(self, job):
    self.job = job

  def executeTask(self):
    raise Exception("Needs to be implemented")
