package ru.yandex.cloud.ml.platform.lzy.model.snapshot;

import java.net.URI;

public interface WhiteboardInfo {
    URI id();
    WhiteboardStatus.State state();

    class Impl implements WhiteboardInfo {
        private final URI id;
        private final WhiteboardStatus.State state;

        public Impl(URI id, WhiteboardStatus.State state) {
            this.id = id;
            this.state = state;
        }

        @Override
        public URI id() {
            return id;
        }

        @Override
        public WhiteboardStatus.State state() {
            return state;
        }
    }
}
