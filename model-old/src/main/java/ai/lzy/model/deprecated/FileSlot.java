package ai.lzy.model.deprecated;

import ai.lzy.model.slot.Slot;
import java.nio.file.Path;

@Deprecated
public interface FileSlot extends Slot {
    Path mount();

    @Override
    default Media media() {
        return Media.FILE;
    }
}
