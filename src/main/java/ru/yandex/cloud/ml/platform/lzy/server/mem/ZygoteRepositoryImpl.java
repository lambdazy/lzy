package ru.yandex.cloud.ml.platform.lzy.server.mem;

import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.server.ZygoteRepository;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class ZygoteRepositoryImpl implements ZygoteRepository {
    private final Map<String, Zygote> operations = new HashMap<>();

    @Override
    public synchronized boolean publish(String name, Zygote workload) {
        return operations.putIfAbsent(name, workload) == null;
    }

    @Override
    public Stream<String> list() {
        return operations.keySet().stream();
    }

    @Override
    public Zygote get(String name) {
        return operations.get(name);
    }
}
