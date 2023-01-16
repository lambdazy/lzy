package ai.lzy.whiteboard.grpc;

import ai.lzy.v1.whiteboard.LWB;
import ai.lzy.whiteboard.model.Field;
import ai.lzy.whiteboard.model.Whiteboard;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


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
            .setUri(storage.uri().toString())
            .build();
    }

    public static LWB.WhiteboardField toProto(Field field) {
        return LWB.WhiteboardField.newBuilder()
            .setName(field.name())
            .setScheme(ai.lzy.model.grpc.ProtoConverter.toProto(field.scheme()))
            .build();
    }

    public static Whiteboard fromProto(LWB.Whiteboard whiteboard) {
        return new Whiteboard(whiteboard.getId(), whiteboard.getName(), fromProto(whiteboard.getFieldsList()),
            new HashSet<>(whiteboard.getTagsList()),
            fromProto(whiteboard.getStorage()), whiteboard.getNamespace(),
            Whiteboard.Status.valueOf(whiteboard.getStatus().name()),
            ai.lzy.util.grpc.ProtoConverter.fromProto(whiteboard.getCreatedAt()));
    }

    public static Map<String, Field> fromProto(List<LWB.WhiteboardField> fieldList) {
        return fieldList.stream()
            .map(ProtoConverter::fromProto)
            .collect(Collectors.toMap(Field::name, x -> x));
    }

    public static Field fromProto(LWB.WhiteboardField field) {
        return new Field(field.getName(), ai.lzy.model.grpc.ProtoConverter.fromProto(field.getScheme()));
    }

    public static Whiteboard.Storage fromProto(LWB.Storage storage) {
        return new Whiteboard.Storage(storage.getName(), storage.getDescription(), URI.create(storage.getUri()));
    }
}
