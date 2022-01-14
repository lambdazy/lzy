package ru.yandex.cloud.ml.platform.lzy.backoffice.models.whiteboards;

import io.micronaut.core.annotation.Introspected;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.UserCredentials;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;

@Introspected
public class WhiteboardsCommand {

    private UserCredentials credentials;

    public UserCredentials getCredentials() {
        return credentials;
    }

    public void setCredentials(UserCredentials credentials) {
        this.credentials = credentials;
    }

    public LzyWhiteboard.WhiteboardsCommand toModel(IAM.UserCredentials credentials) {
        return LzyWhiteboard.WhiteboardsCommand.newBuilder()
                .setBackoffice(LzyWhiteboard.BackofficeCredentials.newBuilder()
                .setCredentials(this.credentials.toModel())
                .setBackofficeCredentials(credentials))
                .build();
    }
}
