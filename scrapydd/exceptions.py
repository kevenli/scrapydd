
class ProjectNotFound(Exception):
    pass


class SpiderNotFound(Exception):
    pass


class NodeExpired(Exception):
    pass


class JobRunning(Exception):
    def __init__(self, jobid):
        super(JobRunning, self).__init__(self)
        self.jobid=jobid

class InvalidCronExpression(Exception):
    pass
