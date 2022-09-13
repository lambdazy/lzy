package ai.lzy.site.configs;

import ai.lzy.v1.deprecated.LzyAuth;

public interface CredentialsProvider {

    LzyAuth.UserCredentials createCreds();
}
