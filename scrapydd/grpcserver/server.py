import asyncio
from concurrent import futures
import logging
import grpc
import sys
from . import service_pb2
from . import service_pb2_grpc
from .grpc_asyncio import AsyncioExecutor
from ..nodes import NodeManager


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
    def __init__(self, node_manager: NodeManager):
        self._node_manager = node_manager

    def get_node_id(self, context):
        for key, value in context.invocation_metadata():
            if key == 'x-node-id':
                return value

    async def Heartbeat(self, request, context):
        node_id = self.get_node_id(context)
        logger.debug('heartbeat, node: %s', node_id)
        self._node_manager.heartbeat(node_id)
        return service_pb2.HeartbeatResponse()


def start(node_manager):
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    port = '1337'
    with open('keys/localhost.key', 'rb') as f:
        private_key = f.read()
    with open('keys/localhost.crt', 'rb') as f:
        certificate_chain = f.read()

    server_credentials = grpc.ssl_server_credentials(
        ((private_key, certificate_chain,),))

    server = grpc.server(AsyncioExecutor(loop=asyncio.new_event_loop()))
    #server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    #server = grpc.server(AsyncioExecutor())
    service_pb2_grpc.add_NodeServiceServicer_to_server(NodeServicer(node_manager), server)

    address = '[::]:' + port
    logger.info('starting grpc server on %s', address)
    server.add_secure_port(address, server_credentials)

    server.start()
    return server