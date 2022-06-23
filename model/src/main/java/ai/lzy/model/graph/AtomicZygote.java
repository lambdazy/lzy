package ai.lzy.model.graph;


import ai.lzy.model.Zygote;
import ai.lzy.priv.v2.Operations;

public interface AtomicZygote extends Zygote {
    Env env();

    String description();

    String fuze();

    Provisioning provisioning();

    Operations.Zygote zygote();
}
