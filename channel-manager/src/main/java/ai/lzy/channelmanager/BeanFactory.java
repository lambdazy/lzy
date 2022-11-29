package ai.lzy.channelmanager;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.longrunning.OperationService;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.longrunning.dao.OperationDaoImpl;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Factory
public class BeanFactory {

    @Singleton
    public GrainedLock lockManager(ChannelManagerConfig config) {
        return new GrainedLock(config.getLockBucketsCount());
    }

    @Singleton
    @Requires(beans = ChannelManagerDataSource.class)
    @Named("ChannelManagerOperationDao")
    public OperationDao operationDao(ChannelManagerDataSource dataSource) {
        return new OperationDaoImpl(dataSource);
    }

    @Singleton
    @Named("ChannelManagerOperationService")
    public OperationService operationService(OperationDao operationDao) {
        return new OperationService(operationDao);
    }

}
