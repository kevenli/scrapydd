import asyncio
from concurrent import futures
import logging
import grpc
import sys
from . import service_pb2
from . import service_pb2_grpc
from .grpc_asyncio import AsyncioExecutor


logger = logging.getLogger(__name__)


class NodeServicer(service_pb2_grpc.NodeServiceServicer):
    async def Heartbeat(self, request, context):
        logger.debug('heartbeat')
        return service_pb2.HeartbeatResponse()


def start():
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
    service_pb2_grpc.add_NodeServiceServicer_to_server(NodeServicer(), server)

    address = '[::]:' + port
    logger.info('starting grpc server on %s', address)
    server.add_secure_port(address, server_credentials)

    server.start()
    return server