
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


class ProcessFailed(Exception):
    def __init__(self, err_output = None, std_output=None):
        super(ProcessFailed, self).__init__(self, 'Error when running process.')
        self.err_output = err_output
        self.std_output = std_output


class InvalidProjectEgg(Exception):
    def __init__(self, message = None, detail = None):
        if message is None:
            message = 'Invalid project egg.'
        super(InvalidProjectEgg, self).__init__(self, message)
        self.message = message
        self.detail = detail
