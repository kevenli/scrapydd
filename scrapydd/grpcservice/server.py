import asyncio
import datetime
from io import BytesIO
import logging
import os
import re
import sys

import grpc

from . import service_pb2
from . import service_pb2_grpc
from .grpc_asyncio import AsyncioExecutor
from ..nodes import NodeManager, NodeExpired, AnonymousNodeDisabled
from .. import nodes
from ..schedule import SchedulerManager
from ..models import session_scope, SpiderSettings, SpiderExecutionQueue, \
    Session
from ..models import NodeKey, NodeSession
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
                return int(value)

    async def HeartbeatNodeSession(self,
                                   request: service_pb2.HeartbeatNodeSessionRequest,
                                   context):
        """

        :param request:
        :param context:
        :return:


            possible exceptions:
                NodeSession not found (404 NOT_FOUND):
                    The specified NodeSession according to the session_id
                    parameter is not found.

                Node not found (404 NOT_FOUND):
                    A Node corresponding to the NodeSession is None, this may
                    be caused by manually deleted by the admin user.

                Token invalid (401 UNAUTHENTICATED):
                    The token provided in Headers is invalid or is not
                    correctly match the session or node.

        """
        with session_scope() as session:
            resource_name = request.name
            node_session_id = int(re.search(r'^nodeSessions/(\d+)$',
                                            resource_name).group(1))
            response = service_pb2.HeartbeatNodeSessionResponse()
            node_session = session.query(NodeSession).get(node_session_id)
            if node_session is None:
                context.set_code(grpc.StatusCode.UNAUTHENTICATED)
                context.set_details('Session not found.')
                return response
            node_id = node_session.node_id
            logger.debug('heartbeat, node: %s, session: %s', node_id,
                         node_session.id)
            has_task = self._scheduler_manager.has_task(node_id)
            try:
                self._node_manager.node_session_heartbeat(session,
                                                          node_session.id)
                running_job_ids = request.running_job_ids
                killing_jobs = list(
                    self._scheduler_manager.jobs_running(node_id,
                                                         running_job_ids))

                response.new_job_available = has_task
                for killing_job in killing_jobs:
                    response.kill_job_ids.append(killing_job)
            except NodeExpired:
                context.set_code(grpc.StatusCode.UNAUTHENTICATED)
                context.set_details('Node expired.')
                return response
            return response

    async def ObtainNodeSessionJob(self, request, context):
        resource_name = request.name
        m = re.search(r'nodeSessions/(\d+)/jobs', resource_name)
        session_id = int(m.group(1))
        response = service_pb2.NodeSessionJob()
        node_manager = self._node_manager
        with session_scope() as session:
            node_session = node_manager.get_node_session(session, session_id)
            next_task = self._scheduler_manager.get_next_task(
                node_session.node_id)

            if not next_task:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('No job available.')
                return response

            figure = self._project_manager.get_job_figure(session, next_task)
            name = 'nodeSessions/%s/jobs/%s' % (node_session.id,
                                                 next_task.id)
            response.name = name
            response.id = next_task.id
            response.figure = figure.to_json()
            return response

    async def CompleteNodeSessionJob(self, request, context):
        resource_name = request.name
        m = re.search(r'nodeSessions/(\d+)/jobs/(\w+)', resource_name)
        node_session_id = int(m.group(1))
        task_id = m.group(2)
        status = request.status

        response = service_pb2.CompleteNodeSessionJobResponse()
        if status == 'success':
            status_int = 2
        elif status == 'fail':
            status_int = 3
        else:
            logger.warning('invalid job status %s %s', node_id, status)
            return response

        with session_scope() as session:
            node_session = session.query(NodeSession).get(node_session_id)
            if not node_session:
                context.set_code(grpc.StatusCode.UNAUTHENTICATED)
                context.set_details('Node session does not exist.')
                return response

            query = session.query(SpiderExecutionQueue) \
                .filter(SpiderExecutionQueue.id == task_id,
                        SpiderExecutionQueue.status.in_([1, 5]))

            job = query.first()

            if job is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('job not found.')
                return response

            log_stream = None
            if request.logs:
                logger.debug('logs file length %s', len(request.logs))
                log_stream = BytesIO(request.logs)

            items_stream = None
            if request.items:
                logger.debug('items file length %s', len(request.items))
                items_stream = BytesIO(request.items)

            job.status = status_int
            job.update_time = datetime.datetime.now()
            historical_job = self._scheduler_manager.job_finished(job,
                                                                  log_stream,
                                                                  items_stream)
            session.close()
            logger.info('Job %s completed.', task_id)
            return response

    async def CreateNode(self, request, context):
        session = Session()
        node_manager = self._node_manager
        response = service_pb2.Node()
        node_key = request.node_key
        key = session.query(NodeKey).filter_by(key=node_key).first()
        if not key:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid Token')
            return response

        if key.used_node_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid Token')
            return response

        tags = request.node.tags
        # tags = ','.join(tags) if tags else None

        remote_ip = context.peer()
        node = node_manager.create_node(remote_ip, tags=tags,
                                        key_id=key.id)
        key.used_node_id = node.id
        session.add(key)
        session.commit()
        response.name = 'nodes/%s' % node.id
        response.id = node.id
        response.display_name = node.name
        for tag in node.tags:
            response.tags.append(tag)
        response.is_online = node.isalive > 0
        response.client_ip = node.client_ip
        return response

    async def CreateNodeSession(self, request, context):
        response = service_pb2.NodeSession()
        with session_scope() as session:
            node_id = request.node_session.node_id
            remote_ip = context.peer()
            node_manager = self._node_manager
            try:
                node_session = node_manager.create_node_session(
                    session,
                    node=node_id,
                    client_ip=remote_ip)
            except AnonymousNodeDisabled:
                context.set_code(grpc.StatusCode.UNAUTHENTICATED)
                context.set_details('Anonymous node is not allowed.')
                return response
            except nodes.NodeNotFoundException:
                context.set_code(grpc.StatusCode.UNAUTHENTICATED)
                context.set_details('Node not found.')
                return response
            except nodes.LivingNodeSessionExistException:
                context.set_code(grpc.StatusCode.ABORTED)
                context.set_details('There are some NodeSession living already'
                                    ' relates to the target Node.')
                return response

            response.name = 'nodeSessions/%s' % node_session.id
            response.id = node_session.id
            response.node_id = node_session.node.id
            logger.info('Node %s logged in, session_id: %s',
                        node_session.node_id, node_session.id)
            return response

    async def GetNodeSessionJobEgg(self, request, context):
        resource_name = request.name
        m = re.search(r'nodeSessions/(\d+)/jobs/(\w+)/egg', resource_name)
        node_session_id = int(m.group(1))
        job_id = m.group(2)
        with session_scope() as session:
            node_session = session.query(NodeSession).get(node_session_id)
            job = session.query(SpiderExecutionQueue) \
                .filter_by(id=job_id) \
                .first()
            if not job:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Job not found.')
                return

            if job.node_id != node_session.node_id:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Job not found.')
                return

            f_egg = self._project_manager.get_job_egg(session, job)
            buffer_size = 8192
            read_buffer = f_egg.read(buffer_size)
            while read_buffer:
                yield service_pb2.DataChunk(data=read_buffer)
                read_buffer = f_egg.read(buffer_size)
            f_egg.close()


def start(node_manager=None, scheduler_manager=None, project_manager=None):
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    port = '6801'
    grpc_port = 6802
    with open('keys/localhost.key', 'rb') as f:
        private_key = f.read()
    with open('keys/localhost.crt', 'rb') as f:
        certificate_chain = f.read()

    server_credentials = grpc.ssl_server_credentials(
        ((private_key, certificate_chain,),))

    options = [
        ('grpc.max_send_message_length', 50 * 1024 * 1024),
        ('grpc.max_receive_message_length', 50 * 1024 * 1024)
    ]
    server = grpc.server(AsyncioExecutor(loop=asyncio.new_event_loop()),
                         options=options)
    # server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    # server = grpc.server(AsyncioExecutor())
    node_service = NodeServicer(node_manager, scheduler_manager,
                                project_manager)
    service_pb2_grpc.add_NodeServiceServicer_to_server(node_service, server)

    address = '[::]:' + port
    logger.info('starting grpc server on %s', address)
    server.add_secure_port(address, server_credentials)

    grpc_address = '[::]:%s' % grpc_port
    server.add_insecure_port(grpc_address)

    server.start()
    return server
