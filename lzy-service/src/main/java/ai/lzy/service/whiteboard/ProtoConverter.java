package ai.lzy.service.whiteboard;

import ai.lzy.v1.whiteboard.LWB;
import ai.lzy.v1.whiteboard.LWBPS;
import ai.lzy.v1.workflow.LWFS;

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

                if (!field.hasDefault()) {
                    fieldInfo.setNoneState(LWB.WhiteboardFieldInfo.NoneField.getDefaultInstance());
                } else {
                    var def = field.getDefault();
                    fieldInfo.setLinkedState(LWB.WhiteboardFieldInfo.LinkedField.newBuilder()
                        .setScheme(def.getDataScheme())
                        .setStorageUri(def.getUri())
                        .build());
                }

                return fieldInfo.build();
            }).toList())
            .build();
    }

    public static LWBPS.LinkFieldRequest newLWBPSLinkFieldRequest(LWFS.LinkWhiteboardRequest request) {
        return LWBPS.LinkFieldRequest.newBuilder()
            .setWhiteboardId(request.getWhiteboardId())
            .setFieldName(request.getFieldName())
            .setStorageUri(request.getStorageUri())
            .setScheme(request.getDataScheme())
            .build();
    }
}
