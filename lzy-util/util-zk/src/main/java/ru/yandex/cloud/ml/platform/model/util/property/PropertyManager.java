package ru.yandex.cloud.ml.platform.model.util.property;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Objects;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.Value;

public interface PropertyManager<T> {

    void addConfigChangedListener(Consumer<T> listener);

    T getConfig();

    void updateConfig(T config);

    class WorkerConfig {

        @JsonProperty("backendTag")
        private final String backendTag;
        @JsonProperty("copyJupyterKernelTag")
        private final String copyJupyterKernelTag;
        @JsonProperty("userTag")
        private final String userTag;

        @JsonCreator
        public WorkerConfig(
            @JsonProperty("backendTag") String backendTag,
            @JsonProperty("copyJupyterKernelTag") String copyJupyterKernelTag,
            @JsonProperty("userTag") String userTag
        ) {
            this.backendTag = backendTag;
            this.copyJupyterKernelTag = copyJupyterKernelTag;
            this.userTag = userTag;
        }


        public String backendTag() {
            return backendTag;
        }

        public String copyJupyterKernelTag() {
            return copyJupyterKernelTag;
        }

        public String userTag() {
            return userTag;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final WorkerConfig that = (WorkerConfig) o;
            return backendTag.equals(that.backendTag)
                && copyJupyterKernelTag.equals(that.copyJupyterKernelTag)
                && userTag.equals(that.userTag);
        }


        @Override
        public int hashCode() {
            return Objects.hash(backendTag, copyJupyterKernelTag, userTag);
        }

        @Override
        public String toString() {
            return "WorkerConfig{"
                + "backendTag='" + backendTag + '\''
                + ", copyJupyterKernelTag='" + copyJupyterKernelTag + '\''
                + ", userTag='" + userTag + '\''
                + '}';
        }
    }

    @Builder(toBuilder = true)
    class RoomConfig {

        @JsonProperty("roomTag")
        private final String roomTag;

        @JsonProperty("pushClientTag")
        private final String pushClientTag;

        @JsonProperty("metricsAgentTag")
        private final String metricsAgentTag;

        public RoomConfig(
            @JsonProperty("roomTag") String roomTag,
            @JsonProperty("pushClientTag") String pushClientTag,
            @JsonProperty("metricsAgentTag") String metricsAgentTag
        ) {
            this.roomTag = roomTag;
            this.pushClientTag = pushClientTag;
            this.metricsAgentTag = metricsAgentTag;
        }

        public RoomConfig copy() {
            return new RoomConfig(
                this.roomTag,
                this.pushClientTag,
                this.metricsAgentTag
            );
        }

        public String roomTag() {
            return roomTag;
        }

        public String pushClientTag() {
            return pushClientTag;
        }

        public String metricsAgentTag() {
            return metricsAgentTag;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final RoomConfig that = (RoomConfig) o;
            return roomTag.equals(that.roomTag)
                && pushClientTag.equals(that.pushClientTag);
        }

        @Override
        public int hashCode() {
            return Objects.hash(roomTag, pushClientTag);
        }

        @Override
        public String toString() {
            return "RoomConfig{" + "roomTag='" + roomTag + '\'' + '}';
        }
    }

    @Value
    @JsonDeserialize(builder = SiteConfig.Builder.class)
    @Builder(builderClassName = "Builder", toBuilder = true)
    class SiteConfig {

        @JsonProperty
        String siteTag;

        public SiteConfig copy() {
            return new SiteConfig(this.siteTag);
        }
    }
}
