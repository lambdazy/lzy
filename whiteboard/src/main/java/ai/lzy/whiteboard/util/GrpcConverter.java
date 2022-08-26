package ai.lzy.whiteboard.util;

import ai.lzy.util.grpc.ProtoConverter;
import ai.lzy.v1.LWBS;
import ai.lzy.whiteboard.model.Whiteboard;

public class GrpcConverter {

    public static LWBS.Whiteboard to(Whiteboard whiteboard) {
        return LWBS.Whiteboard.newBuilder()
            .setId(whiteboard.id())
            .addAllFields(whiteboard.createdFieldNames().stream().map(GrpcConverter::toProtoField).toList())
            .addAllFields(whiteboard.linkedFields().stream().map(GrpcConverter::toProtoField).toList())
            .addAllTags(whiteboard.tags())
            .setStorage(to(whiteboard.storage()))
            .setStatus(LWBS.Whiteboard.Status.valueOf(whiteboard.status().name()))
            .setNamespace(whiteboard.namespace())
            .setCreatedAt(ProtoConverter.toProto(whiteboard.createdAt()))
            .build();
    }

    public static LWBS.Storage to(Whiteboard.Storage storage) {
        return LWBS.Storage.newBuilder()
            .setName(storage.name())
            .setDescription(storage.description())
            .build();
    }

    public static LWBS.WhiteboardField toProtoField(String fieldName) {
        return LWBS.WhiteboardField.newBuilder()
            .setFieldName(fieldName)
            .setCreatedState(LWBS.WhiteboardField.CreatedField.getDefaultInstance())
            .build();
    }

    public static LWBS.WhiteboardField toProtoField(Whiteboard.LinkedField field) {
        return LWBS.WhiteboardField.newBuilder()
            .setFieldName(field.name())
            .setLinkedState(LWBS.WhiteboardField.LinkedField.newBuilder()
                .setScheme(ai.lzy.model.GrpcConverter.to(field.schema()))
                .setStorageUri(field.storageUri()))
            .build();
    }

}
