package ai.lzy.server.mem;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import ai.lzy.model.Zygote;
import ai.lzy.server.ZygoteRepository;

public class ZygoteRepositoryImpl implements ZygoteRepository {
    private final Map<String, Zygote> operations = new ConcurrentHashMap<>();

    @Override
    public boolean publish(String name, Zygote workload) {
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
