package ai.lzy.whiteboard.grpc;

import static ai.lzy.model.GrpcConverter.contentTypeFrom;

import ai.lzy.v1.LWB;
import ai.lzy.whiteboard.model.Field;
import ai.lzy.whiteboard.model.LinkedField;
import ai.lzy.whiteboard.model.Whiteboard;

public class ProtoConverter {

    public static LWB.Whiteboard toProto(Whiteboard whiteboard) {
        return LWB.Whiteboard.newBuilder()
            .setId(whiteboard.id())
            .setName(whiteboard.name())
            .addAllFields(whiteboard.fields().values().stream().map(ProtoConverter::toProto).toList())
            .addAllTags(whiteboard.tags())
            .setStorage(toProto(whiteboard.storage()))
            .setStatus(LWB.Whiteboard.Status.valueOf(whiteboard.status().name()))
            .setNamespace(whiteboard.namespace())
            .setCreatedAt(ai.lzy.util.grpc.ProtoConverter.toProto(whiteboard.createdAt()))
            .build();
    }

    public static LWB.Storage toProto(Whiteboard.Storage storage) {
        return LWB.Storage.newBuilder()
            .setName(storage.name())
            .setDescription(storage.description())
            .build();
    }

    public static LWB.WhiteboardField toProto(Field field) {
        return LWB.WhiteboardField.newBuilder()
            .setStatus(LWB.WhiteboardField.Status.valueOf(field.status().name()))
            .setInfo(LWB.WhiteboardFieldInfo.newBuilder()
                .setName(field.name())
                .setNoneState(LWB.WhiteboardFieldInfo.NoneField.getDefaultInstance())
                .build()
            ).build();
    }

    public static LWB.WhiteboardField toProto(LinkedField field) {
        return LWB.WhiteboardField.newBuilder()
            .setStatus(LWB.WhiteboardField.Status.valueOf(field.status().name()))
            .setInfo(LWB.WhiteboardFieldInfo.newBuilder()
                .setName(field.name())
                .setLinkedState(LWB.WhiteboardFieldInfo.LinkedField.newBuilder()
                    .setStorageUri(field.storageUri())
                    .setScheme(ai.lzy.model.GrpcConverter.to(field.schema()))
                    .build())
                .build())
            .build();
    }

    public static Field fromProto(LWB.WhiteboardFieldInfo fieldInfo) {
        return switch (fieldInfo.getStateCase()) {
            case NONESTATE -> new Field(fieldInfo.getName(), Field.Status.CREATED);
            case LINKEDSTATE -> new LinkedField(fieldInfo.getName(), Field.Status.CREATED,
                fieldInfo.getLinkedState().getStorageUri(), contentTypeFrom(fieldInfo.getLinkedState().getScheme()));
            default -> throw new IllegalArgumentException("Unexpected whiteboard field state "
                                                          + fieldInfo.getStateCase());
        };
    }

    public static Whiteboard.Storage fromProto(LWB.Storage storage) {
        return new Whiteboard.Storage(storage.getName(), storage.getDescription());
    }

}
