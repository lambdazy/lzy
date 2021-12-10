package ru.yandex.cloud.ml.platform.lzy.server.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

import javax.validation.constraints.NotBlank;

@ConfigurationProperties("tasks")
public class TasksConfig {
    private final LocalProcessTaskConfig localProcessTaskConfig;
    @NotBlank
    private TaskType taskType;

    public TasksConfig(LocalProcessTaskConfig localProcessTaskConfig) {
        this.localProcessTaskConfig = localProcessTaskConfig;
    }

    public LocalProcessTaskConfig localProcessTaskConfig() {
        return localProcessTaskConfig;
    }

    public TaskType taskType() {
        return taskType;
    }

    public void setTaskType(TaskType taskType) {
        this.taskType = taskType;
    }

    public enum TaskType {
        LOCAL_DOCKER("local-docker"),
        LOCAL_PROCESS("local-process"),
        KUBER("kuber");

        private final String value;

        TaskType(String value) {

            this.value = value;
        }

        public String value() {
            return value;
        }
    }

    @ConfigurationProperties("local-process")
    public static class LocalProcessTaskConfig {
        @NotBlank
        private String servantJarPath;

        public String servantJarPath() {
            return servantJarPath;
        }

        public void setServantJarPath(String servantJarPath) {
            this.servantJarPath = servantJarPath;
        }
    }
}
