import asyncio
import logging
from copy import deepcopy
from typing import Callable, List, Optional, Sequence, Tuple
from unittest.mock import Mock

import grpc
from grpc.aio._server import Server

from ray.exceptions import RayActorError, RayTaskError
from ray.serve._private.constants import (
    DEFAULT_GRPC_SERVER_OPTIONS,
    RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.proxy_request_response import ResponseStatus
from ray.serve.config import gRPCOptions
from ray.serve.exceptions import BackPressureError, DeploymentUnavailableError
from ray.serve.generated.serve_pb2_grpc import add_RayServeAPIServiceServicer_to_server

logger = logging.getLogger(SERVE_LOGGER_NAME)


class gRPCGenericServer(Server):
    """Custom gRPC server that will override all service method handlers.

    Original implementation see: https://github.com/grpc/grpc/blob/
        60c1701f87cacf359aa1ad785728549eeef1a4b0/src/python/grpcio/grpc/aio/_server.py
    """

    def __init__(
        self,
        service_handler_factory: Callable,
        *,
        extra_options: Optional[List[Tuple[str, str]]] = None,
    ):
        super().__init__(
            thread_pool=None,
            generic_handlers=(),
            interceptors=(),
            maximum_concurrent_rpcs=None,
            compression=None,
            options=DEFAULT_GRPC_SERVER_OPTIONS + (extra_options or []),
        )
        self.generic_rpc_handlers = []
        self.service_handler_factory = service_handler_factory

    def add_generic_rpc_handlers(
        self, generic_rpc_handlers: Sequence[grpc.GenericRpcHandler]
    ):
        """Override generic_rpc_handlers before adding to the gRPC server.

        This function will override all user defined handlers to have
            1. None `response_serializer` so the server can pass back the
            raw protobuf bytes to the user.
            2. `unary_unary` is always calling the unary function generated via
            `self.service_handler_factory`
            3. `unary_stream` is always calling the streaming function generated via
            `self.service_handler_factory`
        """
        serve_rpc_handlers = {}
        rpc_handler = generic_rpc_handlers[0]
        for service_method, method_handler in rpc_handler._method_handlers.items():
            serve_method_handler = method_handler._replace(
                response_serializer=None,
                unary_unary=self.service_handler_factory(
                    service_method=service_method,
                    stream=False,
                ),
                unary_stream=self.service_handler_factory(
                    service_method=service_method,
                    stream=True,
                ),
            )
            serve_rpc_handlers[service_method] = serve_method_handler
        generic_rpc_handlers[0]._method_handlers = serve_rpc_handlers
        self.generic_rpc_handlers.append(generic_rpc_handlers)
        super().add_generic_rpc_handlers(generic_rpc_handlers)


async def start_grpc_server(
    service_handler_factory: Callable,
    grpc_options: gRPCOptions,
    *,
    event_loop: asyncio.AbstractEventLoop,
    enable_so_reuseport: bool = False,
) -> asyncio.Task:
    """Start a gRPC server that handles requests with the service handler factory.

    Returns a task that blocks until the server exits (e.g., due to error).
    """
    from ray.serve._private.default_impl import add_grpc_address

    server = gRPCGenericServer(
        service_handler_factory,
        extra_options=[("grpc.so_reuseport", str(int(enable_so_reuseport)))],
    )
    add_grpc_address(server, f"[::]:{grpc_options.port}")

    # Add built-in gRPC service and user-defined services to the server.
    # We pass a mock servicer because the actual implementation will be overwritten
    # in the gRPCGenericServer implementation.
    mock_servicer = Mock()
    for servicer_fn in [
        add_RayServeAPIServiceServicer_to_server
    ] + grpc_options.grpc_servicer_func_callable:
        servicer_fn(mock_servicer, server)

    await server.start()
    return event_loop.create_task(server.wait_for_termination())


def get_grpc_response_status(
    exc: BaseException, request_timeout_s: float, request_id: str
) -> ResponseStatus:
    if isinstance(exc, TimeoutError):
        message = f"Request timed out after {request_timeout_s}s."
        return ResponseStatus(
            code=grpc.StatusCode.DEADLINE_EXCEEDED,
            is_error=True,
            message=message,
        )
    elif isinstance(exc, asyncio.CancelledError):
        message = f"Client for request {request_id} disconnected."
        return ResponseStatus(
            code=grpc.StatusCode.CANCELLED,
            is_error=True,
            message=message,
        )
    elif isinstance(exc, BackPressureError):
        return ResponseStatus(
            code=grpc.StatusCode.RESOURCE_EXHAUSTED,
            is_error=True,
            message=exc.message,
        )
    elif isinstance(exc, DeploymentUnavailableError):
        if isinstance(exc, RayTaskError):
            logger.warning(f"Request failed: {exc}", extra={"log_to_stderr": False})
        return ResponseStatus(
            code=grpc.StatusCode.UNAVAILABLE,
            is_error=True,
            message=exc.message,
        )
    else:
        if isinstance(exc, (RayActorError, RayTaskError)):
            logger.warning(f"Request failed: {exc}", extra={"log_to_stderr": False})
        else:
            logger.exception("Request failed due to unexpected error.")
        return ResponseStatus(
            code=grpc.StatusCode.INTERNAL,
            is_error=True,
            message=str(exc),
        )


def set_grpc_code_and_details(
    context: grpc._cython.cygrpc._ServicerContext, status: ResponseStatus
):
    # Only the latest code and details will take effect. If the user already
    # set them to a truthy value in the context, skip setting them with Serve's
    # default values. By default, if nothing is set, the code is 0 and the
    # details is "", which both are falsy. So if the user did not set them or
    # if they're explicitly set to falsy values, such as None, Serve will
    # continue to set them with our default values.
    if not context.code():
        context.set_code(status.code)
    if not context.details():
        context.set_details(status.message)


def set_proxy_default_grpc_options(grpc_options) -> gRPCOptions:
    grpc_options = deepcopy(grpc_options) or gRPCOptions()

    if grpc_options.request_timeout_s or RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S:
        grpc_options.request_timeout_s = (
            grpc_options.request_timeout_s or RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S
        )

    return grpc_options
