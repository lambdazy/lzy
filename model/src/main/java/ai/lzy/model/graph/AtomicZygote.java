package ai.lzy.model.graph;

import ai.lzy.model.Zygote;
import ai.lzy.v1.Operations;

public interface AtomicZygote extends Zygote {
    Env env();

    String description();

    String fuze();

    Provisioning provisioning();

    Operations.Zygote zygote();
}
