package ru.yandex.cloud.ml.platform.lzy.iam.authorization;

import java.util.List;
import java.util.stream.Stream;

import ru.yandex.cloud.ml.platform.lzy.iam.resources.AccessBinding;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AccessBindingDelta;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthResource;


public interface AccessBindingClient {

    Stream<AccessBinding> listAccessBindings(AuthResource resource);

    void setAccessBindings(AuthResource resource, List<AccessBinding> accessBinding);

    void updateAccessBindings(AuthResource resource, List<AccessBindingDelta> accessBindingDeltas);

}
