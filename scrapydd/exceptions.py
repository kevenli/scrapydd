from tornado.web import HTTPError


class ObjectNotFound(HTTPError):
    def __init__(self):
        super(ObjectNotFound, self, ).__init__(404, 'Object not found.')


class ProjectNotFound(ObjectNotFound):
    pass


class SpiderNotFound(ObjectNotFound):
    pass


class NodeExpired(Exception):
    pass


class ProjectAlreadyExists(Exception):
    pass


class NodeNotFound(HTTPError):
    def __init__(self):
        super(NodeNotFound, self).__init__(404, 'Node not found.')


class JobRunning(Exception):
    def __init__(self, jobid):
        super(JobRunning, self).__init__(self)
        self.jobid=jobid

    def __repr__(self):
        return 'Job already running, running id :%s' % self.jobid

    def __str__(self):
        return 'Job already running, running id :%s' % self.jobid


class InvalidCronExpression(Exception):
    pass


class ProcessFailed(Exception):
    def __init__(self, message='Error when running process.',
                 ret_code=None,
                 err_output=None,
                 std_output=None):
        super(ProcessFailed, self).__init__(self, message)
        self.ret_code = ret_code
        self.message = message
        self.err_output = err_output
        self.std_output = std_output

    def __str__(self):
        return '%s, %s, %s' % (self.message, self.std_output, self.err_output)


class InvalidProjectEgg(Exception):
    def __init__(self, message = None, detail = None):
        if message is None:
            message = 'Invalid project egg.'
        super(InvalidProjectEgg, self).__init__(self, message)
        self.message = message
        self.detail = detail


class WebhookJobFailedError(Exception):
    def __init__(self, executor, message=None, inner_exc=None):
        self.executor = executor
        self.inner_exc = inner_exc
        self.message = message
        super(WebhookJobFailedError, self).__init__(message)


class WebhookJobOverMemoryLimitError(WebhookJobFailedError):
    pass


class WebhookJobJlDecodeError(WebhookJobFailedError):
    pass

class EggFileNotFound(Exception):
    pass