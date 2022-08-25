package ai.lzy.whiteboard.storage;

import ai.lzy.model.db.NotFoundException;
import ai.lzy.model.db.ReadMode;
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

    void linkField(String userId, String whiteboardId, String fieldName, Whiteboard.LinkedField linkedField,
                   Instant linkedAt, @Nullable TransactionHandle transaction) throws SQLException;

    void setWhiteboardFinalized(String userId, String whiteboardId, Instant finalizedAt,
                                @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Whiteboard findWhiteboard(String userId, String whiteboardId,
                              @Nullable TransactionHandle transaction, ReadMode readMode) throws SQLException;

    default Whiteboard getWhiteboard(
        String userId, String whiteboardId, @Nullable TransactionHandle transaction, ReadMode readMode
    ) throws NotFoundException, SQLException {
        final var whiteboard = findWhiteboard(userId, whiteboardId, transaction, readMode);
        if (whiteboard == null) {
            throw new NotFoundException("Whiteboard " + whiteboardId + " not found");
        }
        return whiteboard;
    }

    Stream<Whiteboard> listWhiteboards(String userId, List<String> tags,
                                       @Nullable TransactionHandle transaction, ReadMode readMode) throws SQLException;

}
