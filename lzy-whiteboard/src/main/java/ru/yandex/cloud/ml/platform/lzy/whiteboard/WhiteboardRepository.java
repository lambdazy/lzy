package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import ru.yandex.cloud.ml.platform.lzy.model.snapshot.*;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.stream.Stream;

public interface WhiteboardRepository {
    void create(Whiteboard whiteboard);
    @Nullable
    WhiteboardStatus resolveWhiteboard(URI id);
    List<WhiteboardInfo> whiteboards(URI uid);

    void add(WhiteboardField field);
    Stream<WhiteboardField> dependent(WhiteboardField field);
    Stream<WhiteboardField> fields(Whiteboard whiteboard);
    List<String> whiteboardsByType(URI uid, String type);
}
