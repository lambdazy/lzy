package ru.yandex.cloud.ml.platform.lzy.iam.authorization;

import java.util.List;
import java.util.stream.Stream;

import ru.yandex.cloud.ml.platform.lzy.model.iam.AccessBinding;
import ru.yandex.cloud.ml.platform.lzy.model.iam.AccessBindingDelta;
import ru.yandex.cloud.ml.platform.lzy.model.iam.AuthResource;


public interface AccessBindingClient {

    Stream<AccessBinding> listAccessBindings(AuthResource resource);

    void setAccessBindings(AuthResource resource, List<AccessBinding> accessBinding);

    void updateAccessBindings(AuthResource resource, List<AccessBindingDelta> accessBindingDeltas);

}
