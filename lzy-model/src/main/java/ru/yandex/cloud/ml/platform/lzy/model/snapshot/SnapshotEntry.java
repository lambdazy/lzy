package ru.yandex.cloud.ml.platform.lzy.model.snapshot;

import java.net.URI;

public interface SnapshotEntry {
    String id();
    URI storage();
    Snapshot snapshot();

    class Impl implements SnapshotEntry {
        private final String id;
        private final URI storage;
        private final Snapshot snapshot;

        public Impl(String id, URI storage, Snapshot snapshot) {
            this.id = id;
            this.storage = storage;
            this.snapshot = snapshot;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public URI storage() {
            return storage;
        }

        @Override
        public Snapshot snapshot() {
            return snapshot;
        }
    }
}
