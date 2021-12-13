package ru.yandex.cloud.ml.platform.lzy.model.snapshot;

public interface WhiteboardField {
    String name();
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
