package ru.yandex.cloud.ml.platform.lzy.model.snapshot;

public interface SnapshotEntry {
    String id();

    Snapshot snapshot();

    class Impl implements SnapshotEntry {
        private final String id;
        private final Snapshot snapshot;

        public Impl(String id, Snapshot snapshot) {
            this.id = id;
            this.snapshot = snapshot;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public Snapshot snapshot() {
            return snapshot;
        }

        public String toString() {
            return "entry id: " + id + ", snapshot: " + snapshot.toString();
        }
    }
}
