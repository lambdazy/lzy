package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.model.Vm;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Yaml;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.*;

@Singleton
@Requires(property = "allocator.kuber-allocator.enabled", value = "true")
public class VmPodProvider {
    private static final Logger LOG = LogManager.getLogger(VmPodProvider.class);

    private final ServiceConfig config;

    @Inject
    public VmPodProvider(ServiceConfig config) {
        this.config = config;
    }


}
