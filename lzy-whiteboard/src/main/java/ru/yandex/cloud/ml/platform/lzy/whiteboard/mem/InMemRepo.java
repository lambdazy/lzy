package ru.yandex.cloud.ml.platform.lzy.whiteboard.mem;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.*;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.SnapshotRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.WhiteboardRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.DbStorage;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Singleton
@Requires(missingBeans = DbStorage.class)
public class InMemRepo implements WhiteboardRepository, SnapshotRepository {
    private final Map<URI, SnapshotStatus> snapshots = new HashMap<>();
    private final Map<URI, List<URI>> snapshotWhiteboardsMapping = new HashMap<>();
    private final Map<URI, Map<String, SnapshotEntry>> entries = new HashMap<>();
    private final Map<URI, List<URI>> snapshotWhiteboardsMapping = new HashMap<>();

    private final Map<URI, WhiteboardStatus> whiteboards = new HashMap<>();
    private final Map<URI, Map<String, WhiteboardField>> fields = new HashMap<>();
    private final Map<URI, Map<String, String>> entryFieldMapping = new HashMap<>();

    @Override
    public synchronized void create(Snapshot snapshot) {
        snapshots.putIfAbsent(snapshot.id(), new SnapshotStatus.Impl(snapshot, SnapshotStatus.State.CREATED));
        entries.putIfAbsent(snapshot.id(), new HashMap<>());
        snapshotWhiteboardsMapping.putIfAbsent(snapshot.id(), new ArrayList<>());
    }

    @Override
    public synchronized SnapshotStatus resolveSnapshot(URI uri) {
        return snapshots.get(uri);
    }

    @Override
    public synchronized void prepare(SnapshotEntry entry) {
        if (!snapshots.containsKey(entry.snapshot().id())) {
            throw new IllegalArgumentException("Snapshot is not found: " + entry.snapshot().id());
        }
        entries.get(entry.snapshot().id()).put(entry.id(), entry);
    }

    @Override
    public synchronized SnapshotEntry resolveEntry(Snapshot snapshot, String id) {
        if (!snapshots.containsKey(snapshot.id())) {
            throw new IllegalArgumentException("Snapshot is not found: " + snapshot.id());
        }
        return entries.get(snapshot.id()).get(id);
    }

    @Override
    public synchronized void commit(SnapshotEntry entry) {
        if (!snapshots.containsKey(entry.snapshot().id())) {
            throw new IllegalArgumentException("Snapshot is not found: " + entry.snapshot().id());
        }
        final Map<String, SnapshotEntry> entryMap = entries.get(entry.snapshot().id());
        if (!entryMap.containsKey(entry.id())) {
            throw new IllegalArgumentException("Entry is not found: " + entry.id());
        }
        entryMap.put(entry.id(), entry);
    }

    @Override
    public synchronized void finalize(Snapshot snapshot) {
        final SnapshotStatus previous = snapshots.get(snapshot.id());
        if (previous == null) {
            throw new IllegalArgumentException("Snapshot is not found: " + snapshot.id());
        }
        final SnapshotStatus.Impl snapshotStatus = new SnapshotStatus.Impl(snapshot, SnapshotStatus.State.FINALIZED);
        snapshots.put(snapshot.id(), snapshotStatus);
        snapshotWhiteboardsMapping.get(snapshot.id()).forEach(uri -> {
            final WhiteboardStatus prev = whiteboards.get(uri);
            if (prev.whiteboard().fieldNames().size() == entryFieldMapping.get(prev.whiteboard().id()).size()) {
                whiteboards.put(uri, new WhiteboardStatus.Impl(prev.whiteboard(), WhiteboardStatus.State.COMPLETED));
            } else {
                whiteboards.put(uri, new WhiteboardStatus.Impl(prev.whiteboard(), WhiteboardStatus.State.NOT_COMPLETED));
            }
        });
    }

    @Override
    public synchronized void error(Snapshot snapshot) {
        final SnapshotStatus previous = snapshots.get(snapshot.id());
        if (previous == null) {
            throw new IllegalArgumentException("Snapshot is not found: " + snapshot.id());
        }
        final SnapshotStatus.Impl snapshotStatus = new SnapshotStatus.Impl(snapshot, SnapshotStatus.State.ERRORED);
        snapshots.put(snapshot.id(), snapshotStatus);
        snapshotWhiteboardsMapping.get(snapshot.id()).forEach(uri -> {
            final WhiteboardStatus prev = whiteboards.get(uri);
            whiteboards.put(uri, new WhiteboardStatus.Impl(prev.whiteboard(), WhiteboardStatus.State.ERRORED));
        });
    }

//    @Override
//    public synchronized Stream<SnapshotEntry> entries(Snapshot snapshot) {
//        return new ArrayList<>(entries.get(snapshot.id()).values()).stream();
//    }

    @Override
    public synchronized void create(Whiteboard whiteboard) {
        final WhiteboardStatus whiteboardStatus = new WhiteboardStatus.Impl(whiteboard, WhiteboardStatus.State.CREATED);
        whiteboards.putIfAbsent(whiteboard.id(), whiteboardStatus);
        fields.putIfAbsent(whiteboard.id(), new HashMap<>());
        entryFieldMapping.putIfAbsent(whiteboard.id(), new HashMap<>());
        snapshotWhiteboardsMapping.get(whiteboard.snapshot().id()).add(whiteboard.id());
    }

    @Override
    public synchronized WhiteboardStatus resolveWhiteboard(URI id) {
        return whiteboards.get(id);
    }

    @Override
    public synchronized void add(WhiteboardField field) {
        if (!whiteboards.containsKey(field.whiteboard().id())) {
            throw new IllegalArgumentException("Whiteboard is not found: " + field.whiteboard().id());
        }
        fields.get(field.whiteboard().id()).put(field.name(), field);
        entryFieldMapping.get(field.whiteboard().id()).put(field.entry().id(), field.name());
    }

    @Override
    public synchronized Stream<WhiteboardField> dependent(WhiteboardField field) {
        if (!whiteboards.containsKey(field.whiteboard().id())) {
            throw new IllegalArgumentException("Whiteboard is not found: " + field.whiteboard().id());
        }
        return field.entry().dependentEntryIds()
                .stream()
                .map(s -> entryFieldMapping.get(field.whiteboard().id()).get(s))
                .filter(Objects::nonNull)
                .map(s -> fields.get(field.whiteboard().id()).get(s))
                .collect(Collectors.toList())
                .stream();
    }

    @Override
    public synchronized Stream<WhiteboardField> fields(Whiteboard whiteboard) {
        if (!whiteboards.containsKey(whiteboard.id())) {
            throw new IllegalArgumentException("Whiteboard is not found: " + whiteboard.id());
        }
        return new ArrayList<>(fields.get(whiteboard.id()).values()).stream();
    }
}
