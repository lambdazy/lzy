package ai.lzy.site.routes.context;

import ai.lzy.site.routes.Auth;
import io.micronaut.context.ApplicationContext;

public abstract class AuthAwareTestContext {
    protected Auth auth;

    protected abstract ApplicationContext micronautContext();

    public void setUpAuth() {
        auth = micronautContext().getBean(Auth.class);
    }
}
