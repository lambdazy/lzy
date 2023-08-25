import asyncio
import dataclasses
import functools
import json
import time
import warnings
from dataclasses import dataclass
from typing import (
    Any,
    AsyncIterable,
    Awaitable,
    Callable,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)

# noinspection PyPackageRequirements
import grpc.aio as aio
# Copy from util/util-grpc/src/main/java/ai/lzy/util/grpc/ChannelBuilder.java
import jwt
# noinspection PyPackageRequirements
from grpc import StatusCode
from grpc.aio import ClientCallDetails, ClientInterceptor, AioRpcError
# noinspection PyPackageRequirements,PyProtectedMember
from grpc.aio._typing import RequestType, ResponseType

from lzy.exceptions import BadClientVersion
from lzy.logs.config import get_logger
from lzy.version import __version__

KEEP_ALIVE_TIME_MS = 3 * 60 * 1000  # 3 minutes
KEEP_ALIVE_TIMEOUT_MS = 1000


_LOG = get_logger(__name__)

IDEMPOTENCY_HEADER_KEY = "idempotency-key"
REQUEST_ID_HEADER_KEY = "x-request-id"


@dataclass
class RetryConfig:
    max_retry: int = 5
    initial_backoff_ms: int = 1000
    max_backoff_ms: int = 5000
    backoff_multiplier: float = 1.0
    retryable_status_codes: Sequence[StatusCode] = dataclasses.field(
        default_factory=lambda: [StatusCode.UNAVAILABLE, StatusCode.CANCELLED]
    )


def build_channel(
    address: str,
    *,
    service_names: Optional[Sequence[str]] = None,
    enable_retry: bool = False,
    retry_config: RetryConfig = RetryConfig(),
    tls: bool = False,
    interceptors: Optional[Sequence[aio.ClientInterceptor]] = None,
    keepalive_ms: int = KEEP_ALIVE_TIME_MS
) -> aio.Channel:
    options: List[Tuple[str, Any]] = [
        ("grpc.enable_retries", 1 if enable_retry else 0),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.keepalive_time_ms", keepalive_ms),
        ("grpc.keepalive_timeout_ms", KEEP_ALIVE_TIMEOUT_MS),
        ("grpc.initial_reconnect_backoff_ms", retry_config.initial_backoff_ms),
        ("grpc.min_reconnect_backoff_ms", retry_config.initial_backoff_ms),
        ("grpc.max_reconnect_backoff_ms", retry_config.max_backoff_ms)
    ]

    if enable_retry:
        assert service_names is not None, "Service name must be specified"
        service_config = {
            "methodConfig": [
                {
                    "name": [{"service": name}],
                    "retryPolicy": {
                        "maxAttempts": retry_config.max_retry,
                        "initialBackoff": f"{retry_config.initial_backoff_ms / 1000}s",
                        "maxBackoff": f"{retry_config.max_backoff_ms / 1000}s",
                        "backoffMultiplier": retry_config.backoff_multiplier,
                        "retryableStatusCodes": [code.name for code in retry_config.retryable_status_codes],
                    },
                } for name in service_names
            ]
        }
        options.append(
            (
                "grpc.service_config",
                json.dumps(service_config),
            )
        )

    if not tls:
        return aio.insecure_channel(address, options=options, interceptors=interceptors)

    raise NotImplementedError('tls are not supported yet')


InterceptorFunction = Callable[
    [ClientCallDetails, AsyncIterable[RequestType]],
    Awaitable[
        Tuple[
            ClientCallDetails,
            AsyncIterable[RequestType],
            Optional[Callable[[ResponseType], Awaitable]],
        ]
    ],
]

T = TypeVar("T")


async def async_iter(it: Iterable[T]) -> AsyncIterable[T]:
    for i in it:
        yield i


class _GenericUnaryUnaryInterceptor(
    aio.UnaryUnaryClientInterceptor
):
    def __init__(self, interceptor_function: InterceptorFunction):
        self._fn = interceptor_function

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        new_details, new_request_iterator, postprocess = await self._fn(
            client_call_details, async_iter((request,))
        )
        response = None
        async for resp in new_request_iterator:
            response = await continuation(new_details, resp)
        return postprocess(response) if postprocess else response


class _GenericUnaryStreamInterceptor(
    aio.UnaryStreamClientInterceptor
):
    def __init__(self, interceptor_function: InterceptorFunction):
        self._fn = interceptor_function

    async def intercept_unary_stream(self, continuation, client_call_details, request):
        new_details, new_request_iterator, postprocess = await self._fn(
            client_call_details, async_iter((request,))
        )
        response = None
        async for resp in new_request_iterator:
            # noinspection PyUnresolvedReferences
            response = await continuation(new_details, resp)
        return postprocess(response) if postprocess else response


class _GenericStreamUnaryInterceptor(
    aio.StreamUnaryClientInterceptor
):
    def __init__(self, interceptor_function: InterceptorFunction):
        self._fn = interceptor_function

    async def intercept_stream_unary(
        self, continuation, client_call_details, request_iterator
    ):
        new_details, new_request_iterator, postprocess = await self._fn(
            client_call_details, request_iterator
        )
        response = await continuation(new_details, new_request_iterator)
        return postprocess(response) if postprocess else response


class _GenericStreamStreamInterceptor(
    aio.StreamStreamClientInterceptor
):
    def __init__(self, interceptor_function: InterceptorFunction):
        self._fn = interceptor_function

    async def intercept_stream_stream(
        self, continuation, client_call_details, request_iterator
    ):
        new_details, new_request_iterator, postprocess = await self._fn(
            client_call_details, request_iterator
        )
        # noinspection PyUnresolvedReferences
        response_it = await continuation(new_details, new_request_iterator)
        return postprocess(response_it) if postprocess else response_it


def add_headers_interceptor(headers: Mapping[str, str]) -> List[ClientInterceptor]:
    async def intercept(
        details: ClientCallDetails, request_iter: AsyncIterable
    ) -> Tuple[ClientCallDetails, AsyncIterable[RequestType], None]:
        meta = [(k, v) for k, v in headers.items()]
        if details.metadata is not None:
            meta.extend(details.metadata)

        # noinspection PyArgumentList
        new_details = ClientCallDetails(
            details.method,
            details.timeout,
            meta,  # type: ignore
            details.credentials,
            details.wait_for_ready,
        )  # type: ignore

        return new_details, request_iter, None

    return [
        _GenericUnaryUnaryInterceptor(intercept),  # type: ignore
        _GenericUnaryStreamInterceptor(intercept),  # type: ignore
        _GenericStreamUnaryInterceptor(intercept),  # type: ignore
        _GenericStreamStreamInterceptor(intercept)  # type: ignore
    ]


def metadata_with(idempotency_key: str) -> List[Tuple[str, str]]:
    return [(IDEMPOTENCY_HEADER_KEY, idempotency_key)]


def build_token(username: str, key_path: str) -> str:
    with open(key_path, "r") as f:
        private_key = f.read()
        return str(
            jwt.encode(
                {  # TODO(artolord) add renewing of token
                    "iat": time.time(),
                    "nbf": time.time(),
                    "exp": time.time() + 7 * 24 * 60 * 60,  # 7 days
                    "iss": username,
                    "pvd": "GITHUB",
                },
                private_key,
                algorithm="PS256",
            )
        )


def retry(config: RetryConfig, action_name: str):

    def decorator(f: Callable[[Any], Awaitable[T]]):

        @functools.wraps(f)
        async def inner(*args: Any, **kwargs: Any) -> T:
            retry_count = 0
            current_backoff = config.initial_backoff_ms

            while retry_count <= config.max_retry:
                try:
                    return await f(*args, **kwargs)
                except AioRpcError as e:
                    if e.code() in config.retryable_status_codes:
                        warnings.warn(
                            f"Lost connection while {action_name}. Retrying, attempt {retry_count}/{config.max_retry}")

                        await asyncio.sleep(current_backoff / 1000)
                        retry_count += 1
                        current_backoff = min(config.max_backoff_ms, int(current_backoff * config.backoff_multiplier))
                        continue
                    raise e
            raise RuntimeError("Lost connection to lzy servers. Please check your network connection and ty again.")

        return inner

    return decorator


def build_headers(token: str) -> List[ClientInterceptor]:
    return add_headers_interceptor({
        "authorization": f"Bearer {token}",
        "x-client-version": f"pylzy={__version__}"
    })


def redefine_errors(f: Callable[..., Awaitable]) -> Callable[..., Awaitable]:

    @functools.wraps(f)
    async def wrapper(*args, **kwargs):
        try:
            return await f(*args, **kwargs)
        except AioRpcError as e:
            data = e.trailing_metadata().get_all("x-supported-client-versions")

            if not data or len(data) == 0:
                raise

            try:
                supported_versions = json.loads(data[0])
            except Exception as ex:
                _LOG.warning("Cannot parse supported versions from server metadata: %s", data[0], exc_info=True)
                raise ex from e

            minimal_version = supported_versions.get("minimal_supported_version")
            banned_versions = supported_versions.get("blacklisted_versions", [])

            if minimal_version is None:
                raise

            pip_request = ",".join((">=" + minimal_version, *("!=" + ver for ver in banned_versions)))

            raise BadClientVersion(
                f"This version of pylzy is unsupported."
                f" Please run `pip install 'pylzy{pip_request}'` to update pylzy")

    return wrapper
