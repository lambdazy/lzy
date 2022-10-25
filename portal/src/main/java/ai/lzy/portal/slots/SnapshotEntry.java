package ai.lzy.portal.slots;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.portal.LzyPortalApi;

import java.util.Collection;
import javax.annotation.Nullable;

import static ai.lzy.portal.Portal.CreateSlotException;

public interface SnapshotEntry {
    String snapshotId();

    LzyInputSlot setInputSlot(SlotInstance slot) throws CreateSlotException;

    LzyOutputSlot addOutputSlot(SlotInstance slot) throws CreateSlotException;

    boolean removeInputSlot(String slotName);

    boolean removeOutputSlot(String slotName);

    @Nullable
    LzyInputSlot getInputSlot();

    Collection<? extends LzyOutputSlot> getOutputSlots();

    @Nullable
    LzyOutputSlot getOutputSlot(String slotName);

    SnapshotEntryState state();

    default LzyPortalApi.SnapshotEntryStatus toProto() {
        var builder = LzyPortalApi.SnapshotEntryStatus.newBuilder();
        var in = getInputSlot();
        if (in != null) {
            builder.setInputSlot(ProtoConverter.toProto(in.instance()).getSlot());
        }
        for (var out: getOutputSlots()) {
            builder.addOutputSlots(ProtoConverter.toProto(out.instance()).getSlot());
        }

        var state = switch (state()) {
            case INITIALIZING -> LzyPortalApi.SnapshotEntryStatus.SnapshotEntryState.INITIALIZING;
            case DATA_SYNCED -> LzyPortalApi.SnapshotEntryStatus.SnapshotEntryState.DATA_SYNCED;
            case READING_DATA -> LzyPortalApi.SnapshotEntryStatus.SnapshotEntryState.READING_DATA;
            case WRITING_DATA -> LzyPortalApi.SnapshotEntryStatus.SnapshotEntryState.WRITING_DATA;
        };

        builder.setState(state);

        var id = snapshotId();

        builder.setS3(LMS3.S3Locator.newBuilder()

                .build())
    }

    enum SnapshotEntryState {
        INITIALIZING,
        READING_DATA,
        WRITING_DATA,
        DATA_SYNCED
    }
}
