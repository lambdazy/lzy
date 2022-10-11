package ai.lzy.util.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.MethodDescriptor;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DeadlineClientInterceptor implements ClientInterceptor {
    private final Duration timeout;

    private DeadlineClientInterceptor(Duration timeout) {
        this.timeout = timeout;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions, Channel next)
    {
        CallOptions callOptionsWithDeadline = timeout != null ?
            callOptions.withDeadlineAfter(timeout.toMillis(), TimeUnit.MILLISECONDS) : callOptions;
        return next.newCall(method, callOptionsWithDeadline);
    }

    public static DeadlineClientInterceptor fromDuration(Duration timeout) {
        return new DeadlineClientInterceptor(timeout);
    }

}
