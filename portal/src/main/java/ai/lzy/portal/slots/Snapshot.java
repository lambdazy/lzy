package ai.lzy.portal.slots;

import ai.lzy.model.slot.SlotInstance;

import java.util.Collection;
import javax.annotation.Nullable;

import static ai.lzy.portal.Portal.CreateSlotException;

public interface Snapshot {

    SnapshotInputSlot setInputSlot(SlotInstance slot) throws CreateSlotException;

    SnapshotOutputSlot addOutputSlot(SlotInstance slot) throws CreateSlotException;

    boolean removeInputSlot(String slotName);

    boolean removeOutputSlot(String slotName);

    @Nullable
    SnapshotInputSlot getInputSlot();

    Collection<? extends SnapshotOutputSlot> getOutputSlots();

    @Nullable
    SnapshotOutputSlot getOutputSlot(String slotName);
}
