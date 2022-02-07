package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import ru.yandex.cloud.ml.platform.lzy.model.snapshot.*;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.stream.Stream;

public interface WhiteboardRepository {
    WhiteboardStatus create(Whiteboard whiteboard);
    @Nullable
    WhiteboardStatus resolveWhiteboard(URI id);
    List<WhiteboardStatus> resolveWhiteboards(String namespace, List<String> tags);

    void update(WhiteboardField field);
    Stream<WhiteboardField> dependent(WhiteboardField field);
    Stream<WhiteboardField> fields(Whiteboard whiteboard);
}
