package ru.yandex.cloud.ml.platform.lzy.iam.resources;

public interface AuthResources {

    String resourceId();

    String type();

    class Workflow implements AuthResources {

        private final String type;
        private final String resourceId;

        public Workflow(String type, String resourceId) {
            this.type = type;
            this.resourceId = resourceId;
        }

        @Override
        public String resourceId() {
            return resourceId;
        }

        @Override
        public String type() {
            return type;
        }
    }

    class Whiteboard implements AuthResources {

        private final String type;
        private final String resourceId;

        public Whiteboard(String type, String resourceId) {
            this.type = type;
            this.resourceId = resourceId;
        }

        @Override
        public String resourceId() {
            return resourceId;
        }

        @Override
        public String type() {
            return type;
        }
    }
}
