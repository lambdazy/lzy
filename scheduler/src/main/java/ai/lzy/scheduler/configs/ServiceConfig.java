package ai.lzy.scheduler.configs;

import io.micronaut.context.annotation.ConfigurationProperties;
import java.util.Map;

@ConfigurationProperties("service")
public record ServiceConfig(int maxServantsPerWorkflow,
                            Map<String, Integer> provisioningLimits,
                            Integer defaultProvisioningLimit) { }
