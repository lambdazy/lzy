package ai.lzy.model.data;

import java.net.URI;
import ai.lzy.model.Slot;

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
