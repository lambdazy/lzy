package ru.yandex.cloud.ml.platform.lzy.server;

import java.net.URI;
import java.util.stream.Stream;

public interface ServantRepository {
    boolean register(URI uri);
    void unregister(URI uri);

    Stream<URI> list();
}
