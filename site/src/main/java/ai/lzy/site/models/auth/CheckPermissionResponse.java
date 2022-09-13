package ai.lzy.site.models.auth;

import ai.lzy.v1.deprecated.BackOffice;
import io.micronaut.core.annotation.Introspected;

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
