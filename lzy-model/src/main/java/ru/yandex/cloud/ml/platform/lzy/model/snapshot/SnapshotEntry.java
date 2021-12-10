package ru.yandex.cloud.ml.platform.lzy.model.snapshot;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public interface SnapshotEntry {
    String id();
    URI storage();
    Set<String> dependentEntryIds();
    Snapshot snapshot();
    boolean empty();

    class Impl implements SnapshotEntry {
        private String id;
        private URI storage;
        private Set<String> deps;
        private Snapshot snapshot;
        private final boolean empty;

        public Impl(String id, URI storage, Set<String> deps, Snapshot snapshot, boolean empty) {
            this.id = id;
            this.storage = storage;
            this.deps = new HashSet<>(deps);
            this.snapshot = snapshot;
            this.empty = empty;
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

        @Override
        public boolean empty() {
            return empty;
        }
    }
}
