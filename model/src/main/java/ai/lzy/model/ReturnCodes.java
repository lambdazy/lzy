package ai.lzy.model;

public enum ReturnCodes {
    ENVIRONMENT_INSTALLATION_ERROR(97),
    INTERNAL_ERROR(98),
    INTERNAL(99);

    private final int rc;

    ReturnCodes(int rc) {
        this.rc = rc;
    }

    public int getRc() {
        return rc;
    }
}
