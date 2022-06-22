package ru.yandex.cloud.ml.platform.lzy.model.data;


import java.net.URI;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;

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
