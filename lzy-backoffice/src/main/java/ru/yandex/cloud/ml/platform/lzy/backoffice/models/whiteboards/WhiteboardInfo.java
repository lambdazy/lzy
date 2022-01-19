package ru.yandex.cloud.ml.platform.lzy.backoffice.models.whiteboards;

import io.micronaut.core.annotation.Introspected;
import java.util.List;
import java.util.stream.Collectors;

import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;

@Introspected
public class WhiteboardInfo {
    private String wbId;
    private String wbStatus;

    public static WhiteboardInfo fromModel(LzyWhiteboard.WhiteboardInfo wbInfo) {
        WhiteboardInfo wb = new WhiteboardInfo();
        wb.wbId = wbInfo.getId();
        wb.wbStatus = wbInfo.getWhiteboardStatus().toString();
        return wb;
    }

    public String getWbId() {
        return wbId;
    }

    public void setWbId(String wbId) {
        this.wbId = wbId;
    }

    public String getWbStatus() {
        return wbStatus;
    }

    public void setWbStatus(String wbStatus) {
        this.wbStatus = wbStatus;
    }
}
