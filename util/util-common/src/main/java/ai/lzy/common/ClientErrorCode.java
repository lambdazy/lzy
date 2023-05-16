package ai.lzy.common;

public enum ClientErrorCode {
    CLIENT_VERSION_NOT_SUPPORTED(1);

    public final int code;

    ClientErrorCode(int code) {
        this.code = code;
    }
}
