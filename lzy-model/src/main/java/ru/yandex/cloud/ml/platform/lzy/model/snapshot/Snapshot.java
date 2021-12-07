package ru.yandex.cloud.ml.platform.lzy.model.snapshot;

import java.net.URI;

public interface Snapshot {
    URI id();
    URI ownerId();

    class Impl implements Snapshot {
        private final URI id;
        private final URI ownerId;

        public Impl(URI id, URI ownerId) {
            this.id = id;
            this.ownerId = ownerId;
        }

        @Override
        public URI id() {
            return id;
        }

        @Override
        public URI ownerId() {
            return ownerId;
        }
    }
}
