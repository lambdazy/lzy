package ai.lzy.model.deprecated;

import ai.lzy.model.slot.Slot;

import java.net.URI;

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
