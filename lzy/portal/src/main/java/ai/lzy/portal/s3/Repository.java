package ai.lzy.portal.s3;

import java.net.URI;

public interface Repository<T> {
    void put(URI uri, T value);

    T get(URI uri);

    boolean contains(URI uri);

    void remove(URI uri);
}
