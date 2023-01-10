package ai.lzy.jobsutils.configs;

import ai.lzy.model.db.DatabaseConfiguration;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties("jobs")
public class ServiceConfig {

    @ConfigurationBuilder("database")
    private final DatabaseConfiguration database = new DatabaseConfiguration();
}
