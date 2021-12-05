package ru.yandex.cloud.ml.platform.lzy.model.snapshot;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public interface SnapshotEntry {
    String id();
    URI storage();
    Set<String> dependentEntryIds();
    Snapshot snapshot();

    class Impl implements SnapshotEntry {
        private final String id;
        private final URI storage;
        private final Set<String> deps;
        private final Snapshot snapshot;

        public Impl(String id, URI storage, Set<String> deps, Snapshot snapshot) {
            this.id = id;
            this.storage = storage;
            this.deps = new HashSet<>(deps);
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
        public Set<String> dependentEntryIds() {
            return new HashSet<>(deps);
        }

        @Override
        public Snapshot snapshot() {
            return snapshot;
        }
    }
}
