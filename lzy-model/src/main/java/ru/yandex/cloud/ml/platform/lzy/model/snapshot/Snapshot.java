package ru.yandex.cloud.ml.platform.lzy.model.snapshot;

import java.net.URI;

public interface Snapshot {
    URI id();

    class Impl implements Snapshot {
        private final URI id;

        public Impl(URI id) {
            this.id = id;
        }

        @Override
        public URI id() {
            return id;
        }
    }
}
