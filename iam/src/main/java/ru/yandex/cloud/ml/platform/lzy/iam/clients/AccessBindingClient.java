package ru.yandex.cloud.ml.platform.lzy.iam.clients;

import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AccessBinding;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AccessBindingDelta;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthResource;

import java.util.List;
import java.util.stream.Stream;

public interface AccessBindingClient {

    Stream<AccessBinding> listAccessBindings(AuthResource resource) throws AuthException;

    void setAccessBindings(AuthResource resource, List<AccessBinding> accessBinding) throws AuthException;

    void updateAccessBindings(AuthResource resource, List<AccessBindingDelta> accessBindingDeltas) throws AuthException;

}
