package ai.lzy.iam.clients;

import ai.lzy.iam.authorization.credentials.Credentials;
import ai.lzy.iam.authorization.exceptions.AuthException;
import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.AccessBindingDelta;
import ai.lzy.iam.resources.AuthResource;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface AccessBindingClient {

    AccessBindingClient withToken(Supplier<Credentials> tokenSupplier);

    Stream<AccessBinding> listAccessBindings(AuthResource resource) throws AuthException;

    void setAccessBindings(AuthResource resource, List<AccessBinding> accessBinding) throws AuthException;

    void updateAccessBindings(AuthResource resource, List<AccessBindingDelta> accessBindingDeltas) throws AuthException;

}
