package ru.yandex.cloud.ml.platform.lzy.model;

public abstract class StorageCredentials {
    private final String bucket;

    public StorageCredentials(String bucket) {
        this.bucket = bucket;
    }

    public abstract static class AzureCredentials extends StorageCredentials {

        public AzureCredentials(String bucket) {
            super(bucket);
        }

        public abstract String connectionString();

        public Type type(){
            return Type.Azure;
        }
    }

    public abstract static class AmazonCredentials extends StorageCredentials {

        public AmazonCredentials(String bucket) {
            super(bucket);
        }

        public abstract String endpoint();
        public abstract String accessToken();
        public abstract String secretToken();

        public Type type(){
            return Type.Amazon;
        }
    }

    public abstract static class AzureSASCredentials extends StorageCredentials{

        public AzureSASCredentials(String bucket) {
            super(bucket);
        }

        public abstract String signature();
        public abstract String endpoint();

        public Type type(){
            return Type.AzureSas;
        }
    }

    public static class EmptyCredentials extends StorageCredentials{

        public EmptyCredentials() {
            super(null);
        }

        @Override
        Type type() {
            return Type.Empty;
        }
    }

    public enum Type{
        Azure,
        AzureSas,
        Amazon,
        Empty
    }

    abstract Type type();
    String bucket(){
        return bucket;
    }
}
