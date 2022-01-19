package ru.yandex.cloud.ml.platform.lzy.backoffice.models.whiteboards;

import io.micronaut.core.annotation.Introspected;
import java.util.List;
import java.util.stream.Collectors;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;

@Introspected
public class WhiteboardsResponse {

    List<WhiteboardInfo> wbInfos;

    public static WhiteboardsResponse fromModel(LzyWhiteboard.WhiteboardsInfo response) {
        WhiteboardsResponse resp = new WhiteboardsResponse();
        resp.wbInfos = response.getWhiteboardsList().stream().map(WhiteboardInfo::fromModel)
                .collect(Collectors.toList());
        return resp;
    }

    public List<WhiteboardInfo> getWbInfos() {
        return wbInfos;
    }

    public void setWbInfos(List<WhiteboardInfo> wbInfos) {
        this.wbInfos = wbInfos;
    }
}
