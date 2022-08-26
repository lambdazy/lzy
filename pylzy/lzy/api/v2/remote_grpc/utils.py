import dataclasses
import json
from dataclasses import dataclass
from typing import Optional, Sequence, Callable, AsyncIterator, Awaitable, Tuple, Iterable, TypeVar, AsyncIterable, \
    Mapping, Any, List
import grpc.aio as aio

# Copy from util/util-grpc/src/main/java/ai/lzy/util/grpc/ChannelBuilder.java
from grpc.aio import ClientCallDetails, ClientInterceptor
from grpc.aio._typing import RequestType, ResponseType

KEEP_ALIVE_TIME_MINS = 3
IDLE_TIMEOUT_MINS = 5
KEEP_ALIVE_TIMEOUT_SECS = 10

@dataclass
class RetryConfig:
    max_retry: int = 3
    initial_backoff: str = "0.5s"  # in duration format
    max_backoff: str = "2s"  # in duration format
    backoff_multiplier: int = 2
    retryable_status_codes: Sequence[str] = dataclasses.field(default_factory=lambda: ["UNAVAILABLE", "CANCELLED"])


def build_channel(
        address: str, *,
        service_name: Optional[str] = None,
        enable_retry: bool = False,
        retry_config: RetryConfig = RetryConfig(),
        tls: bool = False,
        interceptors: Optional[Sequence[aio.ClientInterceptor]] = None
) -> aio.Channel:

    options: List[Tuple[str, Any]] = [
        ("grpc.enable_retries", 1),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.keepalive_time_ms", KEEP_ALIVE_TIME_MINS * 60 * 1000),
        ("grpc.keepalive_timeout_ms", KEEP_ALIVE_TIMEOUT_SECS * 1000),
        ("grpc.client_idle_timeout_ms", IDLE_TIMEOUT_MINS * 60 * 1000)
    ]

    if enable_retry:
        assert service_name is not None, "Service name must be specified"
        service_config = {
            "methodConfig": [
                {
                    "name": [{"service": service_name}],
                    "retryPolicy": {
                        "maxAttempts": retry_config.max_retry,
                        "initialBackoff": retry_config.initial_backoff,
                        "maxBackoff": retry_config.max_backoff,
                        "backoffMultiplier": retry_config.backoff_multiplier,
                        "retryableStatusCodes": retry_config.retryable_status_codes,
                    },
                }
            ]
        }
        options.append(("grpc.service_config", json.dumps(service_config),))

    if not tls:
        return aio.insecure_channel(address, options=options, interceptors=interceptors)


InterceptorFunction = Callable[[ClientCallDetails, AsyncIterable[RequestType]],
                               Awaitable[Tuple[ClientCallDetails, AsyncIterable[RequestType],
                                               Optional[Callable[[ResponseType], Awaitable]]]]]


T = TypeVar("T")


async def async_iter(it: Iterable[T]) -> AsyncIterable[T]:
    for i in it:
        yield i


class _GenericClientInterceptor(aio.UnaryUnaryClientInterceptor, aio.UnaryStreamClientInterceptor,
                                aio.StreamUnaryClientInterceptor, aio.StreamStreamClientInterceptor):

    def __init__(self, interceptor_function: InterceptorFunction):
        self._fn = interceptor_function

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        new_details, new_request_iterator, postprocess = await self._fn(
            client_call_details, async_iter((request,)))
        response = await continuation(new_details, await new_request_iterator.__anext__())
        return postprocess(response) if postprocess else response

    async def intercept_unary_stream(self, continuation, client_call_details, request):
        new_details, new_request_iterator, postprocess = await self._fn(
            client_call_details, async_iter((request,)))
        response_it = continuation(new_details, await new_request_iterator.__anext__())
        return postprocess(response_it) if postprocess else response_it

    async def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        new_details, new_request_iterator, postprocess = await self._fn(
            client_call_details, request_iterator)
        response = continuation(new_details, new_request_iterator)
        return postprocess(response) if postprocess else response

    async def intercept_stream_stream(self, continuation, client_call_details,
                                request_iterator):
        new_details, new_request_iterator, postprocess = await self._fn(
            client_call_details, request_iterator)
        response_it = continuation(new_details, new_request_iterator)
        return postprocess(response_it) if postprocess else response_it


def add_headers_interceptor(headers: Mapping[str, str]) -> ClientInterceptor:

    async def intercept(details: ClientCallDetails, request_iter: AsyncIterable) \
            -> Tuple[ClientCallDetails, AsyncIterable[RequestType], None]:
        meta = [(k, v) for k, v in headers.items()]
        if details.metadata is not None:
            meta.extend(details.metadata)

        new_details = ClientCallDetails(details.method, details.timeout, details.metadata,  # type: ignore
                                        details.credentials, details.wait_for_ready)  # type: ignore

        return new_details, request_iter, None

    return _GenericClientInterceptor(intercept)

