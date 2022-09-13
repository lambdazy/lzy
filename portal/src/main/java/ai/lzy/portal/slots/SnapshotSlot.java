package ai.lzy.portal.slots;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.model.slot.SlotInstance;

import javax.annotation.Nullable;
import java.util.Collection;

import static ai.lzy.portal.Portal.*;

public interface SnapshotSlot {
    LzyInputSlot setInputSlot(SlotInstance slot) throws CreateSlotException;

    LzyOutputSlot addOutputSlot(SlotInstance slot) throws CreateSlotException;

    boolean removeInputSlot(String slotName);

    boolean removeOutputSlot(String slotName);

    @Nullable
    LzyInputSlot getInputSlot();

    Collection<? extends LzyOutputSlot> getOutputSlots();

    @Nullable
    LzyOutputSlot getOutputSlot(String slotName);
}
