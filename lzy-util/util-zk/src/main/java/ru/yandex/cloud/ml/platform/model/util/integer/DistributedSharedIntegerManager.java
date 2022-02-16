package ru.yandex.cloud.ml.platform.model.util.integer;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

@Lazy
@Service("DistributedSharedIntegerManager")
public class DistributedSharedIntegerManager implements SharedIntegerManager {

    private final CuratorFramework zookeeperClient;
    private final ConcurrentHashMap<String, DistributedSharedInteger> integers;
    private final String prefix;

    @Autowired
    public DistributedSharedIntegerManager(CuratorFramework zookeeperClient) {
        this.zookeeperClient = zookeeperClient;
        this.integers = new ConcurrentHashMap<>();
        this.prefix = "/DistributedSharedIntegerManager-";
    }

    private DistributedSharedIntegerManager(
        CuratorFramework zookeeperClient,
        ConcurrentHashMap<String, DistributedSharedInteger> integers,
        String prefix
    ) {
        this.zookeeperClient = zookeeperClient;
        this.integers = integers;
        this.prefix = prefix;
    }

    @Override
    public SharedIntegerManager withPrefix(String prefix) {
        return new DistributedSharedIntegerManager(zookeeperClient, integers, this.prefix + "-" + prefix);
    }

    @Override
    public SharedInteger get(String key, int defaultValue) {
        return integers.computeIfAbsent(
            this.prefix + "-" + key,
            k -> new DistributedSharedInteger(zookeeperClient, this.prefix + "-" + key, defaultValue)
        );
    }
}
