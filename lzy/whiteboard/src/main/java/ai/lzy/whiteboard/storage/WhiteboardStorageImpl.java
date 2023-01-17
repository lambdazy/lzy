package ai.lzy.whiteboard.storage;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.ProtoObjectMapper;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.v1.common.LMD;
import ai.lzy.whiteboard.model.Field;
import ai.lzy.whiteboard.model.Whiteboard;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import static ai.lzy.model.grpc.ProtoConverter.fromProto;
import static ai.lzy.model.grpc.ProtoConverter.toProto;

public class WhiteboardStorageImpl implements WhiteboardStorage {

    private static final Logger LOG = LogManager.getLogger(WhiteboardStorageImpl.class);

    private final Storage dataSource;
    private final ObjectMapper objectMapper;

    @Inject
    public WhiteboardStorageImpl(WhiteboardDataSource storage) {
        this.dataSource = storage;
        this.objectMapper = new ProtoObjectMapper();
    }

    @Override
    public void registerWhiteboard(String userId, Whiteboard whiteboard, Instant registeredAt,
                                   @Nullable TransactionHandle outerTransaction) throws SQLException
    {
        LOG.debug("Inserting whiteboard (userId={},whiteboardId={})", userId, whiteboard.id());
        try (final TransactionHandle transaction = TransactionHandle.getOrCreate(dataSource, outerTransaction)) {
            insertWhiteboardInfo(userId, whiteboard, registeredAt, transaction);
            insertFields(whiteboard.id(), whiteboard.fields().values().stream().toList(), transaction);
            insertWhiteboardTags(whiteboard.id(), whiteboard.tags(), transaction);

            transaction.commit();
        }
        LOG.debug("Inserting whiteboard (userId={},whiteboardId={}) done", userId, whiteboard.id());
    }

    @Override
    public void deleteWhiteboard(String whiteboardId, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.debug("Deleting whiteboard (whiteboardId={})", whiteboardId);
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement(
                "DELETE FROM whiteboards WHERE whiteboard_id = ?"
            ))
            {
                int index = 0;
                st.setString(++index, whiteboardId);

                int affectedRows = st.executeUpdate();
                if (affectedRows == 0) {
                    throw new NotFoundException("Whiteboard to delete not found");
                }
            }
        });
        LOG.debug("Deleting whiteboard (whiteboardId={}) done", whiteboardId);
    }

    @Nullable
    @Override
    public Whiteboard findWhiteboard(String whiteboardId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        LOG.debug("Finding whiteboard (whiteboardId={})", whiteboardId);
        final AtomicReference<Whiteboard> whiteboard = new AtomicReference<>();
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                SELECT
                    wb.whiteboard_id,
                    wb.whiteboard_name,
                    wb.user_id,
                    wb.storage_name,
                    wb.storage_description,
                    wb.storage_uri,
                    wb.whiteboard_status,
                    wb.namespace,
                    wb.created_at,
                    wb.registered_at,
                    f.field_name as field_name,
                    f.data_scheme as field_data_scheme,
                    t.tags as tags
                FROM whiteboards wb
                INNER JOIN whiteboard_fields f ON wb.whiteboard_id = f.whiteboard_id
                LEFT JOIN (
                    SELECT whiteboard_id, ARRAY_AGG(whiteboard_tag) as tags
                    FROM whiteboard_tags
                    GROUP BY whiteboard_id
                ) t ON wb.whiteboard_id = t.whiteboard_id
                WHERE wb.whiteboard_id = ?
                """)
            )
            {
                int index = 0;
                st.setString(++index, whiteboardId);
                Stream<Whiteboard> whiteboards = parseWhiteboards(st.executeQuery());
                whiteboard.set(whiteboards.findFirst().orElse(null));
            } catch (JsonProcessingException e) {
                throw new SQLException(e);
            }
        });
        LOG.debug("Finding whiteboard (whiteboardId={}) done, {}",
            whiteboardId, whiteboard.get() == null ? "not found" : "found");
        return whiteboard.get();
    }

    @Override
    public Stream<Whiteboard> listWhiteboards(String userId, @Nullable String whiteboardName, List<String> tags,
                                              @Nullable Instant createdAtLowerBound,
                                              @Nullable Instant createdAtUpperBound,
                                              @Nullable TransactionHandle transaction) throws SQLException
    {
        LOG.debug("Listing whiteboards (userId={})", userId);

        AtomicInteger index = new AtomicInteger(0);
        List<StatementModifier> statementConditionsSuffixFillers = new ArrayList<>();
        String statementConditionsSuffix = "WHERE user_id = ?";
        statementConditionsSuffixFillers.add((conn, st) -> {
            st.setString(index.incrementAndGet(), userId);
            return st;
        });
        if (whiteboardName != null) {
            statementConditionsSuffix += " AND whiteboard_name = ?";
            statementConditionsSuffixFillers.add((conn, st) -> {
                st.setString(index.incrementAndGet(), whiteboardName);
                return st;
            });
        }
        if (!tags.isEmpty()) {
            statementConditionsSuffix += " AND tags @> ?";
            statementConditionsSuffixFillers.add((conn, st) -> {
                st.setArray(index.incrementAndGet(), conn.createArrayOf("varchar", tags.toArray()));
                return st;
            });
        }
        if (createdAtLowerBound != null) {
            statementConditionsSuffix += " AND created_at >= ?";
            statementConditionsSuffixFillers.add((conn, st) -> {
                st.setTimestamp(index.incrementAndGet(), Timestamp.from(createdAtLowerBound));
                return st;
            });
        }
        if (createdAtUpperBound != null) {
            statementConditionsSuffix += " AND created_at <= ?";
            statementConditionsSuffixFillers.add((conn, st) -> {
                st.setTimestamp(index.incrementAndGet(), Timestamp.from(createdAtUpperBound));
                return st;
            });
        }

        final String statementSuffix = statementConditionsSuffix;
        final StatementModifier statementSuffixFiller = statementConditionsSuffixFillers.stream()
            .reduce((conn, st) -> st, (f1, f2) -> (conn, st) -> f2.apply(conn, f1.apply(conn, st)));
        final List<Whiteboard> whiteboards = new ArrayList<>();
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                SELECT
                    wb.whiteboard_id,
                    wb.whiteboard_name,
                    wb.user_id,
                    wb.storage_name,
                    wb.storage_description,
                    wb.storage_uri,
                    wb.whiteboard_status,
                    wb.namespace,
                    wb.created_at,
                    wb.registered_at,
                    f.field_name as field_name,
                    f.data_scheme as field_data_scheme,
                    t.tags as tags
                FROM whiteboards wb
                INNER JOIN whiteboard_fields f ON wb.whiteboard_id = f.whiteboard_id
                INNER JOIN (
                    SELECT whiteboard_id, ARRAY_AGG(whiteboard_tag) as tags
                    FROM whiteboard_tags
                    GROUP BY whiteboard_id
                ) t ON wb.whiteboard_id = t.whiteboard_id
                """ + statementSuffix)
            )
            {
                statementSuffixFiller.apply(sqlConnection, st);
                whiteboards.addAll(parseWhiteboards(st.executeQuery()).toList());
            } catch (JsonProcessingException e) {
                throw new SQLException(e);
            }
        });
        LOG.debug("Listing whiteboards (userId={}) done, {} found",
            userId, whiteboards.size());
        return whiteboards.stream();
    }

    private void insertWhiteboardInfo(String userId, Whiteboard whiteboard, Instant ts, TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                INSERT INTO whiteboards(
                    whiteboard_id, whiteboard_name, user_id, storage_name, storage_description, storage_uri,
                    whiteboard_status, namespace, created_at, registered_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)
            )
            {
                int index = 0;
                st.setString(++index, whiteboard.id());
                st.setString(++index, whiteboard.name());
                st.setString(++index, userId);
                st.setString(++index, whiteboard.storage().name());
                st.setString(++index, whiteboard.storage().description());
                st.setString(++index, whiteboard.storage().uri().toString());
                st.setString(++index, whiteboard.status().name());
                st.setString(++index, whiteboard.namespace());
                st.setTimestamp(++index, Timestamp.from(whiteboard.createdAt()));
                st.setTimestamp(++index, Timestamp.from(ts));
                st.executeUpdate();
            }
        });
    }

    private void insertFields(String whiteboardId, List<Field> fields, TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                INSERT INTO whiteboard_fields(
                    whiteboard_id, field_name, data_scheme
                ) VALUES (?, ?, ?)
                """)
            )
            {
                for (final Field field : fields) {
                    int index = 0;
                    st.setString(++index, whiteboardId);
                    st.setString(++index, field.name());
                    String dataSchemeJson = objectMapper.writeValueAsString(toProto(field.scheme()));
                    st.setString(++index, dataSchemeJson);
                    st.addBatch();
                }
                st.executeBatch();
            } catch (JsonProcessingException e) {
                throw new SQLException(e);
            }
        });
    }

    private void insertWhiteboardTags(String whiteboardId, Set<String> tags, TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                INSERT INTO whiteboard_tags(whiteboard_id, whiteboard_tag)
                SELECT ?, tags.t_name
                FROM unnest(?) AS tags(t_name)
                """)
            )
            {
                int index = 0;
                st.setString(++index, whiteboardId);
                st.setArray(++index, sqlConnection.createArrayOf("text", tags.toArray()));
                st.executeUpdate();
            }
        });
    }

    private Stream<Whiteboard> parseWhiteboards(ResultSet rs) throws SQLException, JsonProcessingException {
        Map<String, Whiteboard> whiteboardsById = new HashMap<>();
        while (rs.next()) {
            final String whiteboardId = rs.getString("whiteboard_id");
            if (!whiteboardsById.containsKey(whiteboardId)) {
                final var storage = new Whiteboard.Storage(
                    rs.getString("storage_name"),
                    rs.getString("storage_description"),
                    URI.create(rs.getString("storage_uri"))
                );
                final var status = Whiteboard.Status.valueOf(rs.getString("whiteboard_status"));
                final var createdAt = rs.getTimestamp("created_at").toInstant();
                whiteboardsById.put(whiteboardId, new Whiteboard(
                    whiteboardId, rs.getString("whiteboard_name"), new HashMap<>(), new HashSet<>(),
                    storage, rs.getString("namespace"), status, createdAt
                ));

                final java.sql.Array sqlTags = rs.getArray("tags");
                if (sqlTags != null) {
                    final var tags = (String[]) sqlTags.getArray();
                    whiteboardsById.get(whiteboardId).tags().addAll(Arrays.stream(tags).toList());
                }
            }

            final String fieldName = rs.getString("field_name");
            final var dataSchemeJson = rs.getString("field_data_scheme");
            var dataScheme = fromProto(objectMapper.readValue(dataSchemeJson, LMD.DataScheme.class));
            whiteboardsById.get(whiteboardId).fields().put(fieldName, new Field(fieldName, dataScheme));
        }
        return whiteboardsById.values().stream();
    }

    private interface StatementModifier {
        PreparedStatement apply(Connection sqlConnection, PreparedStatement statement) throws SQLException;
    }
}
