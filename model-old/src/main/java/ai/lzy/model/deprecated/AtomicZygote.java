package ai.lzy.model.deprecated;

import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.Provisioning;
import ai.lzy.v1.deprecated.LzyZygote;

@Deprecated
public interface AtomicZygote extends Zygote {
    Env env();

    String description();

    String fuze();

    Provisioning provisioning();

    LzyZygote.Zygote zygote();
}
