package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import java.util.Date;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.*;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.stream.Stream;

public interface WhiteboardRepository {
    WhiteboardStatus create(Whiteboard whiteboard);
    @Nullable
    WhiteboardStatus resolveWhiteboard(URI id);
    Stream<WhiteboardStatus> resolveWhiteboards(String namespace, List<String> tags,
        Date fromDateUTCIncluded, Date toDateUTCExcluded);

    void update(WhiteboardField field);
    Stream<WhiteboardField> dependent(WhiteboardField field);
    Stream<WhiteboardField> fields(Whiteboard whiteboard);
}
