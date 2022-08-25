package ai.lzy.whiteboard.storage;

import ai.lzy.model.data.DataSchema;
import ai.lzy.model.data.types.SchemeType;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.ProtoObjectMapper;
import ai.lzy.model.db.ReadMode;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.TransactionHandleDelegate;
import ai.lzy.whiteboard.model.Whiteboard;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WhiteboardStorageImpl implements WhiteboardStorage {

    private static final Logger LOG = LogManager.getLogger(WhiteboardStorageImpl.class);

    private final Storage dataSource;
    private final ObjectMapper objectMapper;

    @Inject
    public WhiteboardStorageImpl(WhiteboardDataSource storage, ProtoObjectMapper objectMapper) {
        this.dataSource = storage;
        this.objectMapper = objectMapper;
    }

    @Override
    public void insertWhiteboard(String userId, Whiteboard whiteboard,
                                 @Nullable TransactionHandle outerTransaction) throws SQLException
    {
        LOG.debug("Inserting whiteboard (userId={},whiteboardId={})", userId, whiteboard.id());
        try (final TransactionHandle transaction = new TransactionHandleDelegate(dataSource, outerTransaction)) {
            insertWhiteboardInfo(userId, whiteboard, transaction);
            insertWhiteboardFieldNames(whiteboard.id(), whiteboard.createdFieldNames(), transaction);
            insertWhiteboardTags(whiteboard.id(), whiteboard.tags(), transaction);
        }
        LOG.debug("Inserting whiteboard (userId={},whiteboardId={}) done", userId, whiteboard.id());
    }

    @Override
    public void linkField(String userId, String whiteboardId, String fieldName, Whiteboard.LinkedField linkedField,
                          Instant linkedAt, @Nullable TransactionHandle transaction)
    {
        LOG.debug("Linking field (userId={},whiteboardId={},fieldName={})", userId, whiteboardId, fieldName);
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                UPDATE whiteboard_fields SET
                    field_type = ?,
                    field_type_scheme = ?,
                    storage_uri = ?,
                    linked_at = ?
                WHERE whiteboard_id IN (
                    SELECT whiteboard_id
                    FROM whiteboards
                    WHERE user_id = ? AND whiteboard_id = ?
                ) AND field_name = ?
                """)
            ) {
                int index = 0;
                st.setString(++index, linkedField.schema().typeContent());
                st.setString(++index, linkedField.schema().schemeType().name());
                st.setString(++index, linkedField.storageUri());
                st.setTimestamp(++index, Timestamp.from(linkedAt));

                st.setString(++index, whiteboardId);
                st.setString(++index, fieldName);
                st.executeUpdate();
            }
        });
        LOG.debug("Linking field (userId={},whiteboardId={},fieldName={}) done", userId, whiteboardId, fieldName);
    }

    @Override
    public void setWhiteboardFinalized(String userId, String whiteboardId, Instant finalizedAt,
                                       @Nullable TransactionHandle transaction)
    {
        LOG.debug("Finalizing whiteboard (userId={},whiteboardId={})", userId, whiteboardId);
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement(
                "UPDATE whiteboards SET finalized_at = ? WHERE user_id = ? AND whiteboard_id = ?"
            )) {
                int index = 0;
                st.setTimestamp(++index, Timestamp.from(finalizedAt));

                st.setString(++index, userId);
                st.setString(++index, whiteboardId);
                st.executeUpdate();
            }
        });
        LOG.debug("Finalizing whiteboard (userId={},whiteboardId={}) done", userId, whiteboardId);
    }

    @Nullable
    @Override
    public Whiteboard findWhiteboard(String userId, String whiteboardId,
                                     @Nullable TransactionHandle transaction, ReadMode readMode)
    {
        LOG.debug("Finding whiteboard (userId={},whiteboardId={})", userId, whiteboardId);
        final AtomicReference<Whiteboard> whiteboard = new AtomicReference<>();
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                SELECT
                    wb.whiteboard_id,
                    wb.user_id,
                    wb.storage_name,
                    wb.storage_description,
                    wb.whiteboard_status,
                    wb.namespace,
                    wb.created_at,
                    wb.finalized_at,
                    f.field_name as field_name,
                    f.field_type as field_type,
                    f.field_type_scheme as field_type_scheme,
                    f.storage_uri as field_storage_uri,
                    f.linked_at as field_linked_at
                    t.tags as tags
                FROM whiteboards wb
                INNER JOIN whiteboard_fields f ON wb.whiteboard_id = f.whiteboard_id
                INNER JOIN (
                    SELECT whiteboard_id, ARRAY_AGG(whiteboard_tag) as tags
                    FROM whiteboard_tags
                    GROUP BY whiteboard_id
                ) t ON wb.whiteboard_id = t.whiteboard_id
                WHERE wb.whiteboard_id = ? and wb.user_id = ?
                """ + (ReadMode.FOR_UPDATE.equals(readMode) ? "FOR UPDATE" : ""))
            ) {
                int index = 0;
                st.setString(++index, whiteboardId);
                st.setString(++index, userId);
                Stream<Whiteboard> whiteboards = parseWhiteboards(st.executeQuery());
                whiteboard.set(whiteboards.findFirst().orElse(null));
            }
        });
        LOG.debug("Finding whiteboard (userId={},whiteboardId={}) done, {}",
            userId, whiteboardId, whiteboard.get() == null ? "not found" : "found");
        return whiteboard.get();
    }

    @Override
    public Stream<Whiteboard> listWhiteboards(String userId, List<String> tags,
                                              @Nullable TransactionHandle transaction, ReadMode readMode)
    {
        LOG.debug("Listing whiteboards (userId={},tags=[{}])", userId, String.join(",", tags));
        final List<Whiteboard> whiteboards = new ArrayList<>();
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                SELECT
                    wb.whiteboard_id,
                    wb.user_id,
                    wb.storage_name,
                    wb.storage_description,
                    wb.whiteboard_status,
                    wb.namespace,
                    wb.created_at,
                    wb.finalized_at,
                    f.field_name as field_name,
                    f.field_type as field_type,
                    f.field_type_scheme as field_type_scheme,
                    f.storage_uri as field_storage_uri,
                    f.linked_at as field_linked_at
                    t.tags as tags
                FROM whiteboards wb
                INNER JOIN whiteboard_fields f ON wb.whiteboard_id = f.whiteboard_id
                INNER JOIN (
                    SELECT whiteboard_id, ARRAY_AGG(whiteboard_tag) as tags
                    FROM whiteboard_tags
                    GROUP BY whiteboard_id
                ) t ON wb.whiteboard_id = t.whiteboard_id
                WHERE wb.user_id = ? and wb.whiteboard_id IN (
                    SELECT whiteboard_id FROM whiteboard_tags WHERE whiteboard_tag = ANY(?)
                )
                """ + (ReadMode.FOR_UPDATE.equals(readMode) ? "FOR UPDATE" : ""))
            ) {
                int index = 0;
                st.setString(++index, userId);
                st.setArray(++index, sqlConnection.createArrayOf("text", tags.toArray()));
                whiteboards.addAll(parseWhiteboards(st.executeQuery()).toList());
            }
        });
        LOG.debug("Listing whiteboards (userId={},tags=[{}]) done, {} found",
            userId, String.join(",", tags), whiteboards.size());
        return whiteboards.stream();
    }

    private void insertWhiteboardInfo(String userId, Whiteboard whiteboard, TransactionHandle transaction) {
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                INSERT INTO whiteboards(
                    whiteboard_id, user_id, storage_name, storage_description,
                    whiteboard_status, namespace, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """)
            ) {
                int index = 0;
                st.setString(++index, whiteboard.id());
                st.setString(++index, userId);
                st.setString(++index, whiteboard.storage().name());
                st.setString(++index, whiteboard.storage().description());
                st.setString(++index, whiteboard.status().name());
                st.setString(++index, whiteboard.namespace());
                st.setTimestamp(++index, Timestamp.from(whiteboard.createdAt()));
                st.executeUpdate();
            }
        });
    }

    private void insertWhiteboardFieldNames(String whiteboardId, Set<String> fieldNames,
                                            TransactionHandle transaction)
    {
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                INSERT INTO whiteboard_fields(whiteboard_id, field_name)
                SELECT ?, field.f_name
                FROM unnest(?) AS field(f_name)
                """)
            ) {
                int index = 0;
                st.setString(++index, whiteboardId);
                st.setArray(++index, sqlConnection.createArrayOf("text", fieldNames.toArray()));
                st.executeUpdate();
            }
        });
    }

    private void insertWhiteboardTags(String whiteboardId, Set<String> tags, TransactionHandle transaction) {
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                INSERT INTO whiteboard_tags(whiteboard_id, whiteboard_tag)
                SELECT ?, tags.t_name
                FROM unnest(?) AS tags(t_name)
                """)
            ) {
                int index = 0;
                st.setString(++index, whiteboardId);
                st.setArray(++index, sqlConnection.createArrayOf("text", tags.toArray()));
                st.executeUpdate();
            }
        });
    }

    private Stream<Whiteboard> parseWhiteboards(ResultSet rs) throws SQLException {
        Map<String, Whiteboard> whiteboardsById = new HashMap<>();
        while (rs.next()) {
            final String whiteboardId = rs.getString("whiteboard_id");
            if (!whiteboardsById.containsKey(whiteboardId)) {
                final var storage = new Whiteboard.Storage(
                    rs.getString("storage_name"),
                    rs.getString("storage_description")
                );
                final var status = Whiteboard.Status.valueOf(rs.getString("whiteboard_status"));
                final var createdAt = rs.getTimestamp("created_at").toInstant();
                whiteboardsById.put(whiteboardId, new Whiteboard(
                    whiteboardId, new HashSet<>(), new HashSet<>(), new HashSet<>(),
                    storage, rs.getString("namespace"), status, createdAt
                ));

                final java.sql.Array sqlTags = rs.getArray("tags");
                if (sqlTags != null) {
                    final var tags = (String[]) sqlTags.getArray();
                    whiteboardsById.get(whiteboardId).tags().addAll(Arrays.stream(tags).toList());
                }
            }

            final String fieldName = rs.getString("field_name");
            final java.sql.Timestamp sqlLinkedAt = rs.getTimestamp("field_linked_at");
            if (sqlLinkedAt == null) {
                whiteboardsById.get(whiteboardId).createdFieldNames().add(fieldName);
            } else {
                final String storageUri = rs.getString("storage_uri");
                final String type = rs.getString("field_type");
                final String typeScheme = rs.getString("field_type_scheme");

                if (storageUri == null || type == null || typeScheme == null) {
                    final String errorMessage = String.format("Failed to get whiteboard field %s of whiteboard %s, "
                        + "expected linked field, but some columns are null", fieldName, whiteboardId);
                    LOG.error(errorMessage);
                    throw new IllegalStateException(errorMessage);
                }

                whiteboardsById.get(whiteboardId).linkedFields().add(new Whiteboard.LinkedField(
                    fieldName, storageUri, new DataSchema(SchemeType.valueOf(typeScheme), type)
                ));
            }
        }
        return whiteboardsById.values().stream();
    }
}
