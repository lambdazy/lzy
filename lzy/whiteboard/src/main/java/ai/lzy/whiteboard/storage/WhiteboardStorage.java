package ai.lzy.whiteboard.storage;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.whiteboard.model.Whiteboard;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

public interface WhiteboardStorage {

    void registerWhiteboard(String userId, Whiteboard whiteboard, Instant registeredAt,
                            @Nullable TransactionHandle transaction) throws SQLException;

    void deleteWhiteboard(String whiteboardId, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Whiteboard findWhiteboard(String whiteboardId, @Nullable TransactionHandle transaction)
        throws SQLException;

    default Whiteboard getWhiteboard(String whiteboardId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        final var whiteboard = findWhiteboard(whiteboardId, transaction);
        if (whiteboard == null) {
            throw new NotFoundException("Whiteboard " + whiteboardId + " not found");
        }
        return whiteboard;
    }

    Stream<Whiteboard> listWhiteboards(String userId, @Nullable String whiteboardName, List<String> tags,
                                       @Nullable Instant createdAtLowerBound, @Nullable Instant createdAtUpperBound,
                                       @Nullable TransactionHandle transaction) throws SQLException;

}
