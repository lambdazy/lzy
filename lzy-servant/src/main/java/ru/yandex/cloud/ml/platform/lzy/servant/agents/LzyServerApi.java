package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

public interface LzyServerApi {
    Operations.ZygoteList zygotes(IAM.Auth auth);
}
