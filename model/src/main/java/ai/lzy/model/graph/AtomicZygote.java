package ai.lzy.model.graph;


import ai.lzy.model.Zygote;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

public interface AtomicZygote extends Zygote {
    Env env();

    String description();

    String fuze();

    Provisioning provisioning();

    Operations.Zygote zygote();
}
