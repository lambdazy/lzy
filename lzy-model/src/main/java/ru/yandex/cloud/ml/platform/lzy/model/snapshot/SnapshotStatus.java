package ru.yandex.cloud.ml.platform.lzy.model.snapshot;

public interface SnapshotStatus {
    Snapshot snapshot();
    State state();

    enum State {
        CREATED,
        FINALIZED,
        ERRORED
    }

    class Impl implements SnapshotStatus {
        private final Snapshot snapshot;
        private final State state;

        public Impl(Snapshot snapshot, State state) {
            this.snapshot = snapshot;
            this.state = state;
        }

        @Override
        public Snapshot snapshot() {
            return snapshot;
        }

        @Override
        public State state() {
            return state;
        }
    }
}
