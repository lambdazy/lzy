package ai.lzy.portal.slots;

import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.exceptions.CreateSlotException;

import java.util.Collection;
import javax.annotation.Nullable;

public interface Snapshot {

    SnapshotInputSlot setInputSlot(SlotInstance slot, @Nullable Runnable syncHandler) throws CreateSlotException;

    SnapshotOutputSlot addOutputSlot(SlotInstance slot) throws CreateSlotException;

    boolean removeInputSlot(String slotName);

    boolean removeOutputSlot(String slotName);

    @Nullable
    SnapshotInputSlot getInputSlot();

    Collection<? extends SnapshotOutputSlot> getOutputSlots();

    @Nullable
    SnapshotOutputSlot getOutputSlot(String slotName);
}
