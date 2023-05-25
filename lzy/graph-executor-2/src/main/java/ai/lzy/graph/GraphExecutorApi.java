package ai.lzy.graph;

import ai.lzy.graph.GraphExecutorApi2.*;
import ai.lzy.graph.services.GraphService;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.v1.longrunning.LongRunning;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class GraphExecutorApi extends GraphExecutorGrpc.GraphExecutorImplBase {
    private static final Logger LOG = LogManager.getLogger(GraphExecutorApi.class);

    private final GraphService graphService;
    private final OperationDao operationDao;

    @Inject
    public GraphExecutorApi(GraphService graphService,
                            @Named("GraphExecutorOperationDao") OperationDao operationDao) {
        this.graphService = graphService;
        this.operationDao = operationDao;
    }

    @Override
    public void execute(GraphExecuteRequest request, StreamObserver<LongRunning.Operation> responseObserver) {

    }
}
