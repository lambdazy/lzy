package ru.yandex.cloud.ml.platform.lzy.backoffice.models.auth;

import io.micronaut.core.annotation.Introspected;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;

@Introspected
public class CheckPermissionResponse {

    private boolean granted;

    public static CheckPermissionResponse fromModel(BackOffice.CheckPermissionResponse response) {
        CheckPermissionResponse resp = new CheckPermissionResponse();
        resp.setGranted(response.getGranted());
        return resp;
    }

    public boolean getGranted() {
        return granted;
    }

    public void setGranted(boolean granted) {
        this.granted = granted;
    }
}
