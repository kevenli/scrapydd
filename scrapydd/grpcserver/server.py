import asyncio
import logging
import grpc
import sys
from . import service_pb2
from . import service_pb2_grpc
from .grpc_asyncio import AsyncioExecutor
from ..nodes import NodeManager, NodeExpired
from ..schedule import SchedulerManager
from ..models import session_scope, SpiderSettings, SpiderExecutionQueue
from ..project import ProjectManager
from ..workspace import DictSpiderSettings


logger = logging.getLogger(__name__)


class SignatureValidationInterceptor(grpc.ServerInterceptor):
    def __init__(self):
        def abort(ignored_request, context):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, 'Invalid signature')

        self._abortion = grpc.unary_unary_rpc_method_handler(abort)

    def intercept_service(self, continuation, handler_call_details):
        ticket = None
        for k, v in handler_call_details.invocation_metadata:
            if k == 'x-node-id':
                ticket = v
                break
        if ticket:
            handler_call_details.invocation_metadata.node_id = ticket
            return continuation(handler_call_details)
        else:
            return self._abortion


class NodeServicer(service_pb2_grpc.NodeServiceServicer):
    def __init__(self, node_manager: NodeManager,
                 scheduler_manager: SchedulerManager,
                 project_manager: ProjectManager):
        self._node_manager = node_manager
        self._scheduler_manager = scheduler_manager
        self._project_manager = project_manager

    def get_node_id(self, context):
        for key, value in context.invocation_metadata():
            if key == 'x-node-id':
                return value

    async def Heartbeat(self, request: service_pb2.HeartbeatRequest, context):
        node_id = self.get_node_id(context)
        logger.debug('heartbeat, node: %s', node_id)
        has_task = self._scheduler_manager.has_task(node_id)

        response = service_pb2.HeartbeatResponse()
        try:
            self._node_manager.heartbeat(node_id)
            running_job_ids = request.runningJobs
            killing_jobs = list(self._scheduler_manager.jobs_running(node_id,
                                                                running_job_ids))

            response.newJobAvailable = has_task
            for killing_job in killing_jobs:
                response.killJobs.append(killing_job)
        except NodeExpired:
            response.nodeExpired = True
        return response

    async def GetNextJob(self, request, context):
        node_id = self.get_node_id(context)
        response = service_pb2.GetNextJobResponse()
        with session_scope() as session:
            next_task = self._scheduler_manager.get_next_task(node_id)
            next_task = session.query(SpiderExecutionQueue).get(next_task.id)

            if not next_task:
                return response

            figure = self._project_manager.get_job_figure(session, next_task)
            response.jobId = next_task.id
            response.figure = figure.to_json()
            f_egg = self._project_manager.get_job_egg(session=session,
                                                      job=next_task)
            response.package = f_egg.read()
            f_egg.close()
            return response


def start(node_manager=None, scheduler_manager=None, project_manager=None):
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    port = '6801'
    with open('keys/localhost.key', 'rb') as f:
        private_key = f.read()
    with open('keys/localhost.crt', 'rb') as f:
        certificate_chain = f.read()

    server_credentials = grpc.ssl_server_credentials(
        ((private_key, certificate_chain,),))

    server = grpc.server(AsyncioExecutor(loop=asyncio.new_event_loop()))
    #server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    #server = grpc.server(AsyncioExecutor())
    node_service = NodeServicer(node_manager, scheduler_manager, project_manager)
    service_pb2_grpc.add_NodeServiceServicer_to_server(node_service, server)

    address = '[::]:' + port
    logger.info('starting grpc server on %s', address)
    server.add_secure_port(address, server_credentials)

    server.start()
    return server