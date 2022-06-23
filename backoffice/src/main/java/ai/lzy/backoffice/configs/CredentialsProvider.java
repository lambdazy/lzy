package ai.lzy.backoffice.configs;

import ai.lzy.priv.v2.IAM;

public interface CredentialsProvider {

    IAM.UserCredentials createCreds();
}
