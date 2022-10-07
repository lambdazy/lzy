package ai.lzy.channelmanager;

import ai.lzy.channelmanager.lock.GrainedLock;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

@Factory
public class BeanFactory {

    @Singleton
    public GrainedLock lockManager(ChannelManagerConfig config) {
        return new GrainedLock(config.getLockBucketsCount());
    }

}
