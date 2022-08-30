package ai.lzy.whiteboard.storage;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.whiteboard.model.Whiteboard;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public interface WhiteboardStorage {

    void insertWhiteboard(String userId, Whiteboard whiteboard,
                          @Nullable TransactionHandle transaction) throws SQLException;

    void linkField(String whiteboardId, String fieldName, Whiteboard.LinkedField linkedField, Instant linkedAt,
                   @Nullable TransactionHandle transaction) throws SQLException;

    void setWhiteboardFinalized(String whiteboardId, Instant finalizedAt,
                                @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Whiteboard findWhiteboard(String whiteboardId, @Nullable TransactionHandle transaction)
        throws SQLException;

    default Whiteboard getWhiteboard(String whiteboardId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        final var whiteboard = findWhiteboard(whiteboardId, transaction);
        if (whiteboard == null) {
            throw new SQLException("Whiteboard " + whiteboardId + " not found");
        }
        return whiteboard;
    }

    Stream<Whiteboard> listWhiteboards(String userId, @Nullable String whiteboardName, List<String> tags,
                                       @Nullable Instant createdAtUpperBound, @Nullable Instant createdAtLowerBound,
                                       @Nullable TransactionHandle transaction) throws SQLException;

}
