package ai.lzy.model;

public abstract class StorageCredentials {

    public abstract Type type();

    public enum Type {
        Azure,
        AzureSas,
        Amazon,
        Empty
    }

    public abstract static class AzureCredentials extends StorageCredentials {

        public abstract String connectionString();

        public Type type() {
            return Type.Azure;
        }
    }

    public abstract static class AmazonCredentials extends StorageCredentials {

        public abstract String endpoint();

        public abstract String accessToken();

        public abstract String secretToken();

        public Type type() {
            return Type.Amazon;
        }
    }

    public abstract static class AzureSASCredentials extends StorageCredentials {

        public abstract String signature();

        public abstract String endpoint();

        public Type type() {
            return Type.AzureSas;
        }
    }

    public static class EmptyCredentials extends StorageCredentials {

        @Override
        public Type type() {
            return Type.Empty;
        }
    }
}
