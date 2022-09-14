package ai.lzy.whiteboard.api;

import ai.lzy.model.DataScheme;
import ai.lzy.v1.deprecated.LzyWhiteboard;
import ai.lzy.whiteboard.model.*;
import com.google.protobuf.Timestamp;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class GrpcConverter {

    public static ExecutionSnapshot from(LzyWhiteboard.ExecutionDescription description) {
        ArrayList<ExecutionValue> outputs = new ArrayList<>();
        ArrayList<InputExecutionValue> inputs = new ArrayList<>();
        ExecutionSnapshot execution = new ExecutionSnapshot.ExecutionSnapshotImpl(
            description.getName(),
            description.getSnapshotId(),
            outputs,
            inputs
        );
        description.getOutputList().stream().map(arg ->
            new ExecutionSnapshot.ExecutionValueImpl(arg.getName(), execution.snapshotId(), arg.getEntryId())
        ).forEach(outputs::add);
        description.getInputList().stream().map(arg ->
            new ExecutionSnapshot.InputExecutionValueImpl(arg.getName(), execution.snapshotId(),
                arg.getEntryId(), arg.getHash())
        ).forEach(inputs::add);
        return execution;
    }

    public static SnapshotEntry from(LzyWhiteboard.SnapshotEntry entry, Snapshot snapshot) {
        return new SnapshotEntry.Impl(entry.getEntryId(), snapshot);
    }

    public static Date from(Timestamp date) {
        return Date.from(Instant.ofEpochSecond(date.getSeconds(), date.getNanos()));
    }

    public static DataScheme contentTypeFrom(LzyWhiteboard.DataScheme dataScheme) {
        return new DataScheme(dataScheme.getSchemeType(), "", dataScheme.getType(), Map.of());
    }

    public static LzyWhiteboard.Snapshot to(Snapshot snapshot) {
        return LzyWhiteboard.Snapshot.newBuilder().setSnapshotId(snapshot.id().toString()).build();
    }

    public static LzyWhiteboard.DataScheme to(DataScheme dataScheme) {
        return LzyWhiteboard.DataScheme.newBuilder()
            .setType(dataScheme.schemaContent())
            .setSchemeType(dataScheme.dataFormat())
            .build();
    }

    public static LzyWhiteboard.WhiteboardField to(
        WhiteboardField field,
        List<WhiteboardField> dependent,
        @Nullable SnapshotEntryStatus entryStatus) {
        final LzyWhiteboard.WhiteboardField.Builder builder = LzyWhiteboard.WhiteboardField.newBuilder()
            .setFieldName(field.name())
            .addAllDependentFieldNames(
                dependent.stream().map(WhiteboardField::name).collect(Collectors.toList()));
        if (entryStatus == null) {
            builder.setEmpty(true);
            builder.setStatus(LzyWhiteboard.WhiteboardField.Status.CREATED);
        } else {
            builder.setEmpty(entryStatus.empty());
            final URI storage = entryStatus.storage();
            if (storage != null) {
                builder.setStorageUri(storage.toString());
            }
            switch (entryStatus.status()) {
                case CREATED:
                    builder.setStatus(LzyWhiteboard.WhiteboardField.Status.CREATED);
                    break;
                case IN_PROGRESS:
                    builder.setStatus(LzyWhiteboard.WhiteboardField.Status.IN_PROGRESS);
                    break;
                case FINISHED:
                    builder.setStatus(LzyWhiteboard.WhiteboardField.Status.FINISHED);
                    break;
                case ERRORED:
                    builder.setStatus(LzyWhiteboard.WhiteboardField.Status.ERRORED);
                    break;
                default:
                    builder.setStatus(LzyWhiteboard.WhiteboardField.Status.UNKNOWN);
                    break;
            }

            DataScheme schema = entryStatus.schema();
            if (schema != null) {
                builder.setScheme(GrpcConverter.to(schema));
            }
        }
        return builder.build();
    }

    public static LzyWhiteboard.WhiteboardStatus to(WhiteboardStatus.State state) {
        switch (state) {
            case CREATED:
                return LzyWhiteboard.WhiteboardStatus.CREATED;
            case COMPLETED:
                return LzyWhiteboard.WhiteboardStatus.COMPLETED;
            case ERRORED:
                return LzyWhiteboard.WhiteboardStatus.ERRORED;
            default:
                throw new IllegalArgumentException("Unknown state: " + state);
        }
    }

    public static LzyWhiteboard.ExecutionDescription to(ExecutionSnapshot execution) {
        return LzyWhiteboard.ExecutionDescription.newBuilder()
            .setName(execution.name())
            .setSnapshotId(execution.snapshotId())
            .addAllInput(execution.inputs().map(
                arg -> LzyWhiteboard.InputArgDescription.newBuilder()
                    .setEntryId(arg.entryId())
                    .setName(arg.name())
                    .setHash(arg.hash())
                    .build()
            ).collect(Collectors.toList()))
            .addAllOutput(execution.outputs().map(
                arg -> LzyWhiteboard.OutputArgDescription.newBuilder()
                    .setEntryId(arg.entryId())
                    .setName(arg.name())
                    .build()
            ).collect(Collectors.toList()))
            .build();
    }

}
