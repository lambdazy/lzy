package ai.lzy.iam.utils;

public record GrpcConfig(String host,
                         int port) {

    public static GrpcConfig from(String address) {
        int idx = address.indexOf(':');
        return new GrpcConfig(address.substring(0, idx), Integer.parseInt(address.substring(idx + 1)));
    }
}
