package ru.yandex.cloud.ml.platform.lzy.model;

public enum ReturnCodes {
    SUCCESS(0),
    ENVIRONMENT_INSTALLATION_ERROR(-97),
    EXECUTION_ERROR(-98);

    private final int rc;

    ReturnCodes(int rc) {
        this.rc = rc;
    }

    public int getRc() {
        return rc;
    }
}
