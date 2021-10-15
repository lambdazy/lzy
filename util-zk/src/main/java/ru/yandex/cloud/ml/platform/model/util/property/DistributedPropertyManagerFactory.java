package ru.yandex.cloud.ml.platform.model.util.property;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

@Lazy
@Service("DistributedPropertyManagerFactory")
public class DistributedPropertyManagerFactory implements PropertyManagerFactory {
    private final CuratorFramework zkClient;
    private final ObjectMapper objectMapper;

    public DistributedPropertyManagerFactory(CuratorFramework zkClient,
                                             @Qualifier("objectMapper") ObjectMapper objectMapper) {
        this.zkClient = zkClient;
        this.objectMapper = objectMapper;
    }

    @Override
    public <T> PropertyManager<T> propertyManager(T initialValue,
                                                  Class<T> tClass,
                                                  String configKey,
                                                  boolean updateTagsOnStartup) {
        return new DistributedPropertyManager<>(
            zkClient,
            initialValue,
            tClass,
            configKey,
            objectMapper,
            updateTagsOnStartup
        );
    }
}
