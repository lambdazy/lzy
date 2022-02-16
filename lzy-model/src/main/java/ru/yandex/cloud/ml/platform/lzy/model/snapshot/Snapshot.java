package ru.yandex.cloud.ml.platform.lzy.model.snapshot;

import java.net.URI;

public interface Snapshot {
    URI id();

    URI uid();

    class Impl implements Snapshot {
        private final URI id;
        private final URI uid;

        public Impl(URI id, URI uid) {
            this.id = id;
            this.uid = uid;
        }

        @Override
        public URI id() {
            return id;
        }

        @Override
        public URI uid() {
            return uid;
        }
    }
}
