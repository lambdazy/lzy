package ru.yandex.cloud.ml.platform.lzy.backoffice.configs;

import yandex.cloud.priv.datasphere.v2.lzy.IAM;

public interface CredentialsProvider {

    IAM.UserCredentials createCreds();
}
