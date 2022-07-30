package ai.lzy.backoffice.configs;

import ai.lzy.v1.IAM;

public interface CredentialsProvider {

    IAM.UserCredentials createCreds();
}
