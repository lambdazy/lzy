package ai.lzy.iam.clients;

import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.AccessBindingDelta;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.exceptions.AuthException;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface AccessBindingClient {

    AccessBindingClient withToken(Supplier<Credentials> tokenSupplier);

    Stream<AccessBinding> listAccessBindings(AuthResource resource) throws AuthException;

    void setAccessBindings(AuthResource resource, List<AccessBinding> accessBinding) throws AuthException;

    void updateAccessBindings(AuthResource resource, List<AccessBindingDelta> accessBindingDeltas) throws AuthException;

}
