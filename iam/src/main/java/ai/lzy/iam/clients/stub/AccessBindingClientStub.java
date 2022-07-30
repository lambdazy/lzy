package ai.lzy.iam.clients.stub;

import ai.lzy.iam.authorization.credentials.Credentials;
import ai.lzy.iam.clients.AccessBindingClient;
import ai.lzy.iam.authorization.exceptions.AuthException;
import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.AccessBindingDelta;
import ai.lzy.iam.resources.AuthResource;

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
