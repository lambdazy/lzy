package ai.lzy.allocator.test;

import yandex.cloud.sdk.auth.IamToken;
import yandex.cloud.sdk.auth.provider.CredentialProvider;

import java.time.Instant;

public class TestCredProvider implements CredentialProvider {

    @Override
    public IamToken get() {
        return new IamToken("", Instant.MAX);
    }

    @Override
    public void close() {

    }

}
