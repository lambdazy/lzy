package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import ru.yandex.cloud.ml.platform.lzy.model.snapshot.*;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.stream.Stream;

public interface WhiteboardRepository {
    void create(Whiteboard whiteboard);
    @Nullable
    WhiteboardStatus resolveWhiteboard(URI id);

    void add(WhiteboardField field);
    Stream<WhiteboardField> dependent(WhiteboardField field);
    Stream<WhiteboardField> fields(Whiteboard whiteboard);
    @Nullable
    SnapshotEntryStatus resolveEntryStatus(Snapshot snapshot, String id);
}
