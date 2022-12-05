package ai.lzy.channelmanager;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.config.ChannelManagerConfig;
import ai.lzy.longrunning.OperationService;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.longrunning.dao.OperationDaoImpl;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Factory
public class BeanFactory {

    @Singleton
    public GrainedLock lockManager(ChannelManagerConfig config) {
        return new GrainedLock(config.getLockBucketsCount());
    }

    @Singleton
    @Named("ChannelManagerOperationDao")
    public OperationDao operationDao(ChannelManagerDataSource dataSource) {
        return new OperationDaoImpl(dataSource);
    }

    @Singleton
    public OperationService operationService(
        @Named("ChannelManagerOperationDao") OperationDao operationDao)
    {
        return new OperationService(operationDao);
    }

}
