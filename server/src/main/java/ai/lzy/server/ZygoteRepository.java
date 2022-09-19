package ai.lzy.server;

import ai.lzy.model.deprecated.Zygote;

import java.util.stream.Stream;

public interface ZygoteRepository {
    boolean publish(String name, Zygote zygote);

    Stream<String> list();

    Zygote get(String name);
}
