package ai.lzy.whiteboard.grpc;

import ai.lzy.v1.LWB;
import ai.lzy.whiteboard.model.Whiteboard;

public class ProtoConverter {

    public static LWB.Whiteboard to(Whiteboard whiteboard) {
        return LWB.Whiteboard.newBuilder()
            .setId(whiteboard.id())
            .setName(whiteboard.name())
            .addAllFields(whiteboard.createdFieldNames().stream().map(ProtoConverter::toProtoField).toList())
            .addAllFields(whiteboard.linkedFields().stream().map(ProtoConverter::toProtoField).toList())
            .addAllTags(whiteboard.tags())
            .setStorage(to(whiteboard.storage()))
            .setStatus(LWB.Whiteboard.Status.valueOf(whiteboard.status().name()))
            .setNamespace(whiteboard.namespace())
            .setCreatedAt(ai.lzy.util.grpc.ProtoConverter.toProto(whiteboard.createdAt()))
            .build();
    }

    public static LWB.Storage to(Whiteboard.Storage storage) {
        return LWB.Storage.newBuilder()
            .setName(storage.name())
            .setDescription(storage.description())
            .build();
    }

    public static LWB.WhiteboardField toProtoField(String fieldName) {
        return LWB.WhiteboardField.newBuilder()
            .setFieldName(fieldName)
            .setCreatedState(LWB.WhiteboardField.CreatedField.getDefaultInstance())
            .build();
    }

    public static LWB.WhiteboardField toProtoField(Whiteboard.LinkedField field) {
        return LWB.WhiteboardField.newBuilder()
            .setFieldName(field.name())
            .setLinkedState(LWB.WhiteboardField.LinkedField.newBuilder()
                .setScheme(ai.lzy.model.GrpcConverter.to(field.schema()))
                .setStorageUri(field.storageUri()))
            .build();
    }

}
