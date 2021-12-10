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
        private String id;
        private URI storage;
        private Set<String> deps;
        private Snapshot snapshot;

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

        public void setId(String id) {
            this.id = id;
        }

        public void setStorage(URI storage) {
            this.storage = storage;
        }

        public void setDeps(Set<String> deps) {
            this.deps = deps;
        }

        public void setSnapshot(Snapshot snapshot) {
            this.snapshot = snapshot;
        }
    }
}
