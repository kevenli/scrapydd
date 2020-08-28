# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

from scrapydd.grpcservice import service_pb2 as scrapydd_dot_grpcservice_dot_service__pb2


class NodeServiceStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.Login = channel.unary_unary(
        '/NodeService/Login',
        request_serializer=scrapydd_dot_grpcservice_dot_service__pb2.LoginRequest.SerializeToString,
        response_deserializer=scrapydd_dot_grpcservice_dot_service__pb2.LoginResponse.FromString,
        )
    self.Heartbeat = channel.unary_unary(
        '/NodeService/Heartbeat',
        request_serializer=scrapydd_dot_grpcservice_dot_service__pb2.HeartbeatRequest.SerializeToString,
        response_deserializer=scrapydd_dot_grpcservice_dot_service__pb2.HeartbeatResponse.FromString,
        )
    self.GetNextJob = channel.unary_unary(
        '/NodeService/GetNextJob',
        request_serializer=scrapydd_dot_grpcservice_dot_service__pb2.GetNextJobRequest.SerializeToString,
        response_deserializer=scrapydd_dot_grpcservice_dot_service__pb2.GetNextJobResponse.FromString,
        )
    self.GetJob = channel.unary_unary(
        '/NodeService/GetJob',
        request_serializer=scrapydd_dot_grpcservice_dot_service__pb2.GetJobRequest.SerializeToString,
        response_deserializer=scrapydd_dot_grpcservice_dot_service__pb2.GetJobResponse.FromString,
        )
    self.GetEgg = channel.unary_unary(
        '/NodeService/GetEgg',
        request_serializer=scrapydd_dot_grpcservice_dot_service__pb2.GetJobEggRequest.SerializeToString,
        response_deserializer=scrapydd_dot_grpcservice_dot_service__pb2.GetJobEggRequest.FromString,
        )
    self.CompleteJob = channel.unary_unary(
        '/NodeService/CompleteJob',
        request_serializer=scrapydd_dot_grpcservice_dot_service__pb2.CompleteJobRequest.SerializeToString,
        response_deserializer=scrapydd_dot_grpcservice_dot_service__pb2.CompleteJobResponse.FromString,
        )
    self.RegisterNode = channel.unary_unary(
        '/NodeService/RegisterNode',
        request_serializer=scrapydd_dot_grpcservice_dot_service__pb2.RegisterNodeRequest.SerializeToString,
        response_deserializer=scrapydd_dot_grpcservice_dot_service__pb2.Node.FromString,
        )
    self.CreateNodeSession = channel.unary_unary(
        '/NodeService/CreateNodeSession',
        request_serializer=scrapydd_dot_grpcservice_dot_service__pb2.CreateNodeSessionRequest.SerializeToString,
        response_deserializer=scrapydd_dot_grpcservice_dot_service__pb2.NodeSession.FromString,
        )


class NodeServiceServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def Login(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def Heartbeat(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def GetNextJob(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def GetJob(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def GetEgg(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def CompleteJob(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def RegisterNode(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def CreateNodeSession(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_NodeServiceServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'Login': grpc.unary_unary_rpc_method_handler(
          servicer.Login,
          request_deserializer=scrapydd_dot_grpcservice_dot_service__pb2.LoginRequest.FromString,
          response_serializer=scrapydd_dot_grpcservice_dot_service__pb2.LoginResponse.SerializeToString,
      ),
      'Heartbeat': grpc.unary_unary_rpc_method_handler(
          servicer.Heartbeat,
          request_deserializer=scrapydd_dot_grpcservice_dot_service__pb2.HeartbeatRequest.FromString,
          response_serializer=scrapydd_dot_grpcservice_dot_service__pb2.HeartbeatResponse.SerializeToString,
      ),
      'GetNextJob': grpc.unary_unary_rpc_method_handler(
          servicer.GetNextJob,
          request_deserializer=scrapydd_dot_grpcservice_dot_service__pb2.GetNextJobRequest.FromString,
          response_serializer=scrapydd_dot_grpcservice_dot_service__pb2.GetNextJobResponse.SerializeToString,
      ),
      'GetJob': grpc.unary_unary_rpc_method_handler(
          servicer.GetJob,
          request_deserializer=scrapydd_dot_grpcservice_dot_service__pb2.GetJobRequest.FromString,
          response_serializer=scrapydd_dot_grpcservice_dot_service__pb2.GetJobResponse.SerializeToString,
      ),
      'GetEgg': grpc.unary_unary_rpc_method_handler(
          servicer.GetEgg,
          request_deserializer=scrapydd_dot_grpcservice_dot_service__pb2.GetJobEggRequest.FromString,
          response_serializer=scrapydd_dot_grpcservice_dot_service__pb2.GetJobEggRequest.SerializeToString,
      ),
      'CompleteJob': grpc.unary_unary_rpc_method_handler(
          servicer.CompleteJob,
          request_deserializer=scrapydd_dot_grpcservice_dot_service__pb2.CompleteJobRequest.FromString,
          response_serializer=scrapydd_dot_grpcservice_dot_service__pb2.CompleteJobResponse.SerializeToString,
      ),
      'RegisterNode': grpc.unary_unary_rpc_method_handler(
          servicer.RegisterNode,
          request_deserializer=scrapydd_dot_grpcservice_dot_service__pb2.RegisterNodeRequest.FromString,
          response_serializer=scrapydd_dot_grpcservice_dot_service__pb2.Node.SerializeToString,
      ),
      'CreateNodeSession': grpc.unary_unary_rpc_method_handler(
          servicer.CreateNodeSession,
          request_deserializer=scrapydd_dot_grpcservice_dot_service__pb2.CreateNodeSessionRequest.FromString,
          response_serializer=scrapydd_dot_grpcservice_dot_service__pb2.NodeSession.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'NodeService', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
