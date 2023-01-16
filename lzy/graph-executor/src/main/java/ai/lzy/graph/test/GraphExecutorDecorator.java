package ai.lzy.graph.test;

import ai.lzy.graph.GraphExecutorApi;
import ai.lzy.graph.algo.GraphBuilder;
import ai.lzy.graph.api.SchedulerApi;
import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.GraphExecutionDao;
import ai.lzy.graph.queue.QueueManager;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.util.auth.credentials.RenewableJwt;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import lombok.Setter;

import java.util.function.Consumer;

@Singleton
@Requires(env = "test-mock")
@Primary
@Setter
public class GraphExecutorDecorator extends GraphExecutorApi {
    private volatile Consumer<String> onStop = (graphId) -> {};

    public GraphExecutorDecorator(ServiceConfig config, GraphExecutionDao dao,
                                  @Named("GraphExecutorOperationDao") OperationDao operationDao,
                                  @Named("GraphExecutorIamGrpcChannel") ManagedChannel iamChannel,
                                  @Named("GraphExecutorIamToken") RenewableJwt iamToken,
                                  GraphBuilder graphBuilder, QueueManager queueManager, SchedulerApi schedulerApi)
    {
        super(config, dao, operationDao, iamChannel, iamToken, graphBuilder, queueManager, schedulerApi);
    }

    @Override
    public void stop(ai.lzy.v1.graph.GraphExecutorApi.GraphStopRequest request,
                     StreamObserver<ai.lzy.v1.graph.GraphExecutorApi.GraphStopResponse> responseObserver)
    {
        onStop.accept(request.getGraphId());
        super.stop(request, responseObserver);
    }
}
