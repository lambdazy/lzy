package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import io.grpc.stub.StreamObserver;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class WhiteboardApi extends WbApiGrpc.WbApiImplBase {
    enum WhiteboardStatus {
        CREATED,
        FINALIZED,
        ERRORED
    }

    private static LzyWhiteboard.Whiteboard.WhiteboardStatus to(WhiteboardStatus status) {
        switch (status) {
            case CREATED: {
                return LzyWhiteboard.Whiteboard.WhiteboardStatus.CREATED;
            }
            case FINALIZED: {
                return LzyWhiteboard.Whiteboard.WhiteboardStatus.FINALIZED;
            }
            case ERRORED: {
                return LzyWhiteboard.Whiteboard.WhiteboardStatus.ERRORED;
            }
        }
        return LzyWhiteboard.Whiteboard.WhiteboardStatus.UNKNOWN;
    }

    private final SnapshotApiGrpc.SnapshotApiBlockingStub snapshot;

    public WhiteboardApi(SnapshotApiGrpc.SnapshotApiBlockingStub snapshot) {
        this.snapshot = snapshot;
    }

    private static class FieldMapping {
        private final String fieldName;
        private final String entryId;

        private FieldMapping(String fieldName, String entryId) {
            this.fieldName = fieldName;
            this.entryId = entryId;
        }

        public static LzyWhiteboard.FieldMapping to(FieldMapping fm) {
            return LzyWhiteboard.FieldMapping
                    .newBuilder()
                    .setFieldName(fm.fieldName)
                    .setEntryId(fm.entryId)
                    .build();
        }

        public static FieldMapping to(LzyWhiteboard.FieldMapping fm) {
            return new FieldMapping(fm.getFieldName(), fm.getEntryId());
        }
    }

    private final ConcurrentHashMap<URI, URI> snapshotBindings = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<URI, WhiteboardStatus> wbStatus = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<URI, Set<FieldMapping>> fieldMappings = new ConcurrentHashMap<>();

    @Override
    public void createWhiteboard(LzyWhiteboard.CreateWhiteboardCommand request, StreamObserver<LzyWhiteboard.WhiteboardId> responseObserver) {
        URI wbId = URI.create(UUID.randomUUID().toString());
        snapshotBindings.put(wbId, URI.create(request.getSnapshotId()));
        wbStatus.put(wbId, WhiteboardStatus.CREATED);
        final LzyWhiteboard.WhiteboardId id = LzyWhiteboard.WhiteboardId
                .newBuilder()
                .setWbId(wbId.toString())
                .build();
        responseObserver.onNext(id);
        responseObserver.onCompleted();
    }

    @Override
    public void addLink(LzyWhiteboard.AddLinkCommand request, StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        fieldMappings.putIfAbsent(URI.create(request.getWbId()), new HashSet<>());
        fieldMappings.computeIfPresent(URI.create(request.getWbId()),
                (k, v) -> {
                    for (var entry : request.getMappingsList()) {
                        v.add(FieldMapping.to(entry));
                    }
                    return v;
                });
        final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
                .newBuilder()
                .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
                .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void getWhiteboard(LzyWhiteboard.GetWhiteboardCommand request,
                              StreamObserver<LzyWhiteboard.Whiteboard> responseObserver) {
        LzyWhiteboard.GetAllLinksResponse response = snapshot.getAllLinks(LzyWhiteboard.GetAllLinksCommand
                .newBuilder()
                .setSnapshotId(snapshotBindings.get(URI.create(request.getWbId())).toString())
                .build());
        var fM = fieldMappings.get(URI.create(request.getWbId()));
        List<LzyWhiteboard.StorageBinding> bindings = new ArrayList<>();
        for (var binding : response.getStorageBindingsList()) {
            String fieldName = null;
            String uri = null;
            for (var mapping : fM) {
                if (mapping.entryId.equals(binding.getFieldName())) {
                    fieldName = mapping.fieldName;
                }
            }
            if (fieldName != null) {
                bindings.add(LzyWhiteboard.StorageBinding
                        .newBuilder()
                        .setFieldName(fieldName)
                        .setStorageUri(binding.getStorageUri())
                        .build()
                );
            }
        }
        List<LzyWhiteboard.Relation> relations = new ArrayList<>();
        for (var relation : response.getRelationList()) {
            String fieldName = null;
            String uri = null;
            for (var mapping : fM) {
                if (mapping.entryId.equals(relation.getFieldName())) {
                    fieldName = mapping.fieldName;
                }
            }
            if (fieldName != null) {
                var builder = LzyWhiteboard.Relation.newBuilder();
                builder.setFieldName(fieldName);
                var deps = new ArrayList<String>();
                for (var dep : relation.getDependenciesList()) {
                    String depFieldName = null;
                    for (var mapping : fM) {
                        if (mapping.entryId.equals(dep)) {
                            depFieldName = mapping.fieldName;
                        }
                    }
                    if (depFieldName != null) {
                        deps.add(depFieldName);
                    }
                }
                builder.addAllDependencies(deps);
                relations.add(builder.build());
            }
        }
        final LzyWhiteboard.Whiteboard whiteboard = LzyWhiteboard.Whiteboard
                .newBuilder()
                .addAllStorageBindings(bindings)
                .addAllRelations(relations)
                .setWhiteboardStatus(to(wbStatus.get(URI.create(request.getWbId()))))
                .build();
        responseObserver.onNext(whiteboard);
        responseObserver.onCompleted();
    }
}
