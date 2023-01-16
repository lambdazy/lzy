package ai.lzy.channelmanager.test;

import ai.lzy.channelmanager.dao.ChannelDao;
import ai.lzy.channelmanager.dao.ChannelManagerDataSource;
import ai.lzy.channelmanager.dao.ChannelOperationDao;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.operation.ChannelOperationExecutor;
import ai.lzy.channelmanager.operation.ChannelOperationManager;
import ai.lzy.channelmanager.services.ChannelManagerPrivateService;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.longrunning.LongRunning;
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
public class ChannelManagerDecorator extends ChannelManagerPrivateService {
    private volatile Consumer<String> onDestroyAll = (executionId) -> {};

    public ChannelManagerDecorator(ChannelDao channelDao,
                                   @Named("ChannelManagerOperationDao") OperationDao operationDao,
                                   ChannelOperationDao channelOperationDao, ChannelManagerDataSource storage,
                                   ChannelOperationManager channelOperationManager, GrainedLock lockManager,
                                   ChannelOperationExecutor executor)
    {
        super(channelDao, operationDao, channelOperationDao, storage, channelOperationManager, lockManager, executor);
    }

    @Override
    public void destroyAll(LCMPS.ChannelDestroyAllRequest request, StreamObserver<LongRunning.Operation> response) {
        onDestroyAll.accept(request.getExecutionId());
        super.destroyAll(request, response);
    }
}
