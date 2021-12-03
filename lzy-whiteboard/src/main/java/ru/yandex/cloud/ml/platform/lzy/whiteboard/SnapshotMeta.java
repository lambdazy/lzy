package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

public class SnapshotMeta {
    private static class SlotMapping {
        final String slotName;
        final String entryId;

        private SlotMapping(String slotName, String entryId) {
            this.slotName = slotName;
            this.entryId = entryId;
        }
    }

    private final Set<SlotMapping> slotMappings;

    SnapshotMeta(Set<SlotMapping> slotMappings) {
        this.slotMappings = slotMappings;
    }

    public String getSnapshotId() {
        String entryId = slotMappings.iterator().next().entryId;
        return entryId.substring(0, entryId.indexOf("/"));
    }

    @Nullable
    public String getEntryId(String slotName) {
        for (var entry : slotMappings) {
            if (entry.slotName.equals(slotName)) {
                return entry.entryId;
            }
        }
        return null;
    }

    public static SnapshotMeta from(Tasks.SnapshotMeta meta) {
        HashSet<SlotMapping> mappings = new HashSet<>();
        for (var entry : meta.getMappingsList()) {
            mappings.add(new SlotMapping(entry.getSlotName(), entry.getEntryId()));
        }
        return new SnapshotMeta(mappings);
    }

    public static Tasks.SnapshotMeta to(SnapshotMeta meta) {
        Tasks.SnapshotMeta.Builder builder = Tasks.SnapshotMeta.newBuilder();
        for (var entry : meta.slotMappings) {
            builder.addMappings(LzyWhiteboard.SlotMapping
                    .newBuilder()
                    .setSlotName(entry.slotName)
                    .setEntryId(entry.entryId)
                    .build());
        }
        return builder.build();
    }

    public static SnapshotMeta empty() {
        return new SnapshotMeta(Set.of());
    }
}
