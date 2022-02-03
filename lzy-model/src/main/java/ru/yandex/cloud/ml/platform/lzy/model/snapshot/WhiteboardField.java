package ru.yandex.cloud.ml.platform.lzy.model.snapshot;

import javax.annotation.Nullable;

public interface WhiteboardField {
    String name();
    @Nullable
    SnapshotEntry entry();
    Whiteboard whiteboard();

    class Impl implements WhiteboardField {
        private final String name;
        private final SnapshotEntry entry;
        private final Whiteboard whiteboard;

        public Impl(String name, SnapshotEntry entry, Whiteboard whiteboard) {
            this.name = name;
            this.entry = entry;
            this.whiteboard = whiteboard;
        }

        @Override
        public String name() {
            return name;
        }

        @Nullable
        @Override
        public SnapshotEntry entry() {
            return entry;
        }

        @Override
        public Whiteboard whiteboard() {
            return whiteboard;
        }
    }
}
