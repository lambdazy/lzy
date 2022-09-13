package ai.lzy.site.models.keys;

import ai.lzy.v1.deprecated.BackOffice;
import io.micronaut.core.annotation.Introspected;

import java.util.List;

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
