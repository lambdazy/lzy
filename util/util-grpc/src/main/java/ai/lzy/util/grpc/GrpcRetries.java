package ai.lzy.util.grpc;

public class GrpcRetries {
    public interface RetryableFunction<T> {
        T call();
    }


}
