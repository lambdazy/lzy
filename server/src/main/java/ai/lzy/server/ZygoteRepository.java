package ai.lzy.server;

import java.util.stream.Stream;
import ai.lzy.model.deprecated.Zygote;

public interface ZygoteRepository {
    boolean publish(String name, Zygote zygote);

    Stream<String> list();

    Zygote get(String name);
}
