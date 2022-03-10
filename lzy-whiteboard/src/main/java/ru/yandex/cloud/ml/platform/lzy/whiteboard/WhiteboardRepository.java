package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Whiteboard;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardField;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus;

public interface WhiteboardRepository {

    @NotNull
    WhiteboardStatus create(@NotNull Whiteboard whiteboard);

    Optional<WhiteboardStatus> resolveWhiteboard(@NotNull URI id);

    Stream<WhiteboardStatus> resolveWhiteboards(String namespace, List<String> tags,
        Date fromDateUTCIncluded, Date toDateUTCExcluded);

    void update(WhiteboardField field);

    Stream<WhiteboardField> dependent(WhiteboardField field);

    Stream<WhiteboardField> fields(Whiteboard whiteboard);
}
