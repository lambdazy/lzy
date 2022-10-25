package ai.lzy.service.whiteboard;

import ai.lzy.model.DataScheme;
import ai.lzy.v1.whiteboard.LWB;
import ai.lzy.v1.whiteboard.LWBPS;
import ai.lzy.v1.workflow.LWFS;

import static ai.lzy.model.grpc.ProtoConverter.toProto;

public final class ProtoConverter {
    public static LWBPS.CreateWhiteboardRequest newLWBPSCreateWhiteboardRequest(String userId,
                                                                                LWFS.CreateWhiteboardRequest request)
    {
        return LWBPS.CreateWhiteboardRequest.newBuilder()
            .setUserId(userId)
            .setWhiteboardName(request.getName())
            .setNamespace(request.getNamespace())
            .setStorage(LWB.Storage.newBuilder().setName(request.getStorageName()).build())
            .addAllTags(request.getTagsList())
            .addAllFields(request.getFieldsList().stream().map(field -> {
                var fieldInfo = LWB.WhiteboardFieldInfo.newBuilder()
                    .setName(field.getName());

                if (field.getUri().isBlank()) {
                    fieldInfo.setNoneState(LWB.WhiteboardFieldInfo.NoneField.getDefaultInstance());
                } else {
                    fieldInfo.setLinkedState(LWB.WhiteboardFieldInfo.LinkedField.newBuilder()
                        .setScheme(toProto(DataScheme.PLAIN))
                        .setStorageUri(field.getUri())
                        .build());
                }

                return fieldInfo.build();
            }).toList())
            .build();
    }
}
