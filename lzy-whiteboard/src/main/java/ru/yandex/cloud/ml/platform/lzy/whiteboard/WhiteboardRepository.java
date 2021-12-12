package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Whiteboard;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardField;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus;

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
    boolean empty(WhiteboardField field);
}
