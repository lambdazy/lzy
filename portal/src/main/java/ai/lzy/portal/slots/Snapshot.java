package ai.lzy.portal.slots;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.exceptions.CreateSlotException;

import java.util.Collection;
import javax.annotation.Nullable;

public interface Snapshot {

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
