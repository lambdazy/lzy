package ai.lzy.backoffice.models.keys;

import io.micronaut.core.annotation.Introspected;
import java.util.List;
import ai.lzy.v1.BackOffice;

@Introspected
public class ListKeysResponse {

    private List<String> keyNames;

    public static ListKeysResponse fromModel(BackOffice.ListKeysResponse model) {
        ListKeysResponse resp = new ListKeysResponse();
        resp.keyNames = model.getKeyNamesList();
        return resp;
    }

    public List<String> getKeyNames() {
        return keyNames;
    }

    public void setKeyNames(List<String> keyNames) {
        this.keyNames = keyNames;
    }
}
