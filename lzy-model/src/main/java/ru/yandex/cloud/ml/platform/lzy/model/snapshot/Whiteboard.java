package ru.yandex.cloud.ml.platform.lzy.model.snapshot;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public interface Whiteboard {
    URI id();
    Set<String> fieldNames();
    Snapshot snapshot();

    class Impl implements Whiteboard {
        private final URI id;
        private final Set<String> fieldNames;
        private final Snapshot snapshot;

        public Impl(URI id, Set<String> fieldNames, Snapshot snapshot) {
            this.id = id;
            this.fieldNames = new HashSet<>(fieldNames);
            this.snapshot = snapshot;
        }

        @Override
        public URI id() {
            return id;
        }

        @Override
        public Set<String> fieldNames() {
            return new HashSet<>(fieldNames);
        }

        @Override
        public Snapshot snapshot() {
            return snapshot;
        }
    }
}
