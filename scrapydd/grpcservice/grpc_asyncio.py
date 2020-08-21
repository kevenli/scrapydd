import asyncio
from concurrent import futures
import functools
import inspect
import threading
import logging

from grpc import _server

logger = logging.getLogger(__name__)


def _loop_mgr(loop: asyncio.AbstractEventLoop):
    logger.debug('starting eventloop.')
    asyncio.set_event_loop(loop)
    loop.run_forever()
    logger.debug('loop finished.')
    # If we reach here, the loop was stopped.
    # We should gather any remaining tasks and finish them.
    pending = asyncio.Task.all_tasks(loop=loop)
    if pending:
        loop.run_until_complete(asyncio.gather(*pending))


class AsyncioExecutor(futures.Executor):

    def __init__(self, *, loop=None):

        super().__init__()
        self._shutdown = False
        self._loop = loop or asyncio.get_event_loop()
        self._thread = threading.Thread(target=_loop_mgr, args=(self._loop,),
                                        daemon=True)
        self._thread.start()

    def submit(self, fn, *args, **kwargs):

        if self._shutdown:
            raise RuntimeError('Cannot schedule new futures after shutdown')

        if not self._loop.is_running():
            raise RuntimeError("Loop must be started before any function can "
                               "be submitted")

        if inspect.iscoroutinefunction(fn):
            coro = fn(*args, **kwargs)
            return asyncio.run_coroutine_threadsafe(coro, self._loop)

        else:
            func = functools.partial(fn, *args, **kwargs)
            return self._loop.run_in_executor(None, func)

    def shutdown(self, wait=True):
        self._loop.stop()
        self._shutdown = True
        if wait:
            self._thread.join()


async def _call_behavior(rpc_event, state, behavior, argument, request_deserializer):
    context = _server._Context(rpc_event, state, request_deserializer)
    try:
        return await behavior(argument, context), True
    except Exception as e:  # pylint: disable=broad-except
        with state.condition:
            if e not in state.rpc_errors:
                details = 'Exception calling application: {}'.format(e)
                _server.logging.exception(details)
                _server._abort(state, rpc_event.operation_call,
                       _server.cygrpc.StatusCode.unknown, _server._common.encode(details))
        return None, False

async def _take_response_from_response_iterator(rpc_event, state, response_iterator):
    try:
        return await response_iterator.__anext__(), True
    except StopAsyncIteration:
        return None, True
    except Exception as e:  # pylint: disable=broad-except
        with state.condition:
            if e not in state.rpc_errors:
                details = 'Exception iterating responses: {}'.format(e)
                _server.logging.exception(details)
                _server._abort(state, rpc_event.operation_call,
                       _server.cygrpc.StatusCode.unknown, _server._common.encode(details))
        return None, False

async def _unary_response_in_pool(rpc_event, state, behavior, argument_thunk,
                                  request_deserializer, response_serializer):
    argument = argument_thunk()
    if argument is not None:
        response, proceed = await _call_behavior(rpc_event, state, behavior,
                                                 argument, request_deserializer)
        if proceed:
            serialized_response = _server._serialize_response(
                rpc_event, state, response, response_serializer)
            if serialized_response is not None:
                _server._status(rpc_event, state, serialized_response)

async def _stream_response_in_pool(rpc_event, state, behavior, argument_thunk,
                                   request_deserializer, response_serializer):
    argument = argument_thunk()
    if argument is not None:
        # Notice this calls the normal `_call_behavior` not the awaitable version.
        response_iterator, proceed = _server._call_behavior(
            rpc_event, state, behavior, argument, request_deserializer)
        if proceed:
            while True:
                response, proceed = await _take_response_from_response_iterator(
                    rpc_event, state, response_iterator)
                if proceed:
                    if response is None:
                        _server._status(rpc_event, state, None)
                        break
                    else:
                        serialized_response = _server._serialize_response(
                            rpc_event, state, response, response_serializer)
                        print(response)
                        if serialized_response is not None:
                            print("Serialized Correctly")
                            proceed = _server._send_response(rpc_event, state,
                                                     serialized_response)
                            if not proceed:
                                break
                        else:
                            break
                else:
                    break

_server._unary_response_in_pool = _unary_response_in_pool
_server._stream_response_in_pool = _stream_response_in_pool