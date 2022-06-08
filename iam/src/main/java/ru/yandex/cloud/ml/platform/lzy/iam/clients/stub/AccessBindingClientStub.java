package ru.yandex.cloud.ml.platform.lzy.iam.clients.stub;

import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.Credentials;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.clients.AccessBindingClient;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AccessBinding;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AccessBindingDelta;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthResource;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class AccessBindingClientStub implements AccessBindingClient {
    @Override
    public AccessBindingClient withToken(Supplier<Credentials> tokenSupplier) {
        return this;
    }

    @Override
    public Stream<AccessBinding> listAccessBindings(AuthResource resource) throws AuthException {
        return Stream.empty();
    }

    @Override
    public void setAccessBindings(AuthResource resource, List<AccessBinding> accessBinding) throws AuthException {
    }

    @Override
    public void updateAccessBindings(
            AuthResource resource,
            List<AccessBindingDelta> accessBindingDeltas) throws AuthException {
    }
}
