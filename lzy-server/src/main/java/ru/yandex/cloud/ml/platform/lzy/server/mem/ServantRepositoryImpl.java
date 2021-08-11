package ru.yandex.cloud.ml.platform.lzy.server.mem;

import ru.yandex.cloud.ml.platform.lzy.server.ServantRepository;

import java.net.URI;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class ServantRepositoryImpl implements ServantRepository {
    private final Set<URI> registry = ConcurrentHashMap.newKeySet();

    @Override
    public boolean register(URI uri) {
        return registry.add(uri);
    }

    @Override
    public void unregister(URI uri) {
        registry.remove(uri);
    }

    @Override
    public Stream<URI> list() {
        return registry.stream();
    }
}
