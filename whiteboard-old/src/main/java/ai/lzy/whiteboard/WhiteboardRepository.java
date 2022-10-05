package ai.lzy.whiteboard;

import ai.lzy.whiteboard.exceptions.WhiteboardRepositoryException;
import ai.lzy.whiteboard.model.Whiteboard;
import ai.lzy.whiteboard.model.WhiteboardField;
import ai.lzy.whiteboard.model.WhiteboardStatus;

import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.validation.constraints.NotNull;

public interface WhiteboardRepository {

    @NotNull
    WhiteboardStatus create(@NotNull Whiteboard whiteboard) throws WhiteboardRepositoryException;

    Optional<WhiteboardStatus> resolveWhiteboard(@NotNull URI id);

    Stream<WhiteboardStatus> resolveWhiteboards(String namespace, List<String> tags,
        Date fromDateUTCIncluded, Date toDateUTCExcluded);

    void update(WhiteboardField field) throws WhiteboardRepositoryException;

    Stream<WhiteboardField> dependent(WhiteboardField field);

    Stream<WhiteboardField> fields(Whiteboard whiteboard) throws WhiteboardRepositoryException;
}
