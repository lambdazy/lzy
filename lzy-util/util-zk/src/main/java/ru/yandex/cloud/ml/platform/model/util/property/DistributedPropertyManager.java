package ru.yandex.cloud.ml.platform.model.util.property;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.shared.SharedValue;
import org.apache.curator.framework.recipes.shared.SharedValueListener;
import org.apache.curator.framework.recipes.shared.SharedValueReader;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedPropertyManager<T> implements PropertyManager<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedPropertyManager.class);

    private final ObjectMapper mapper;
    private final List<Consumer<T>> listeners = new ArrayList<>();
    private final Class<? extends T> tClass;
    private SharedValue config;
    private T oldValue;

    public DistributedPropertyManager(CuratorFramework zkClient,
        T initialValue,
        Class<? extends T> tClass,
        String key,
        ObjectMapper mapper,
        boolean updateTagsOnStartup) {
        this(zkClient, initialValue, tClass, "/DistributedPropertyManager", key, mapper, updateTagsOnStartup);
    }

    public DistributedPropertyManager(CuratorFramework zkClient,
        T initialValue,
        Class<? extends T> tClass,
        String prefix,
        String key,
        ObjectMapper mapper,
        boolean updateTagsOnStartup) {
        this.mapper = mapper;
        this.tClass = tClass;
        this.oldValue = initialValue;

        try {
            this.config = new SharedValue(
                zkClient,
                prefix + "/" + key,
                mapper.writeValueAsBytes(initialValue)
            );
            this.config.getListenable().addListener(new SharedValueListener() {
                @Override
                public void valueHasChanged(SharedValueReader sharedValue, byte[] newValue) throws Exception {
                    final T value = mapper.readValue(newValue, tClass);
                    if (oldValue != null && oldValue.equals(value)) {
                        LOGGER.warn("New value equals to the old value!");
                        return;
                    }
                    oldValue = value;
                    LOGGER.info("new config for key " + key + ": " + value);
                    listeners.forEach(l -> l.accept(value));
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                    LOGGER.info("Shared config connection state changed: " + newState.name());
                }
            });
            this.config.start();

            LOGGER.info("Update tags on startup: {}", updateTagsOnStartup);

            if (updateTagsOnStartup) {
                final VersionedValue<byte[]> versionedValue = this.config.getVersionedValue();
                final T curValue = mapper.readValue(versionedValue.getValue(), tClass);

                LOGGER.info("initial value: {}", initialValue);
                LOGGER.info("cur value: {}", curValue);

                if (!initialValue.equals(curValue)) {
                    LOGGER.info("Updating config in ZK...");
                    this.config.trySetValue(versionedValue, mapper.writeValueAsBytes(initialValue));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addConfigChangedListener(Consumer<T> listener) {
        this.listeners.add(listener);
    }

    @Override
    public T getConfig() {
        try {
            return mapper.readValue(config.getValue(), tClass);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateConfig(T config) {
        try {
            this.config.setValue(mapper.writeValueAsBytes(config));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
