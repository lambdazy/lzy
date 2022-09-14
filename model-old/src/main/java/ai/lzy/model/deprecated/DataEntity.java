package ai.lzy.model.deprecated;

import java.net.URI;
import ai.lzy.model.slot.Slot;

@Deprecated
public interface DataEntity extends DataPage {
    Component[] components();

    interface Component {
        URI id();

        Slot source();

        Necessity necessity();

        enum Necessity {
            Needed,
            Supplementary,
            GoodToKnow,
            Temp
        }
    }
}
