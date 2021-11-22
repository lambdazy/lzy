package ru.yandex.cloud.ml.platform.lzy.backoffice.models.tokens;

import io.micronaut.core.annotation.Introspected;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;

import java.util.List;


@Introspected
public class ListTokensResponse {
    private List<String> tokenNames;

    public List<String> getTokenNames() {
        return tokenNames;
    }

    public void setTokenNames(List<String> tokenNames) {
        this.tokenNames = tokenNames;
    }

    public static ListTokensResponse fromModel(BackOffice.ListTokensResponse model){
        ListTokensResponse resp = new ListTokensResponse();
        resp.tokenNames = model.getTokenNamesList();
        return resp;
    }
}
