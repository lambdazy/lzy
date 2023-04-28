package ai.lzy.allocator.services;

import ai.lzy.allocator.alloc.AllocatorMetrics;
import ai.lzy.allocator.alloc.impl.kuber.NodeController;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import jakarta.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

@Controller("/kuber_node")
public class KuberNodeService {
    private static final Logger LOG = LogManager.getLogger(KuberNodeService.class);

    @Inject
    NodeController nodeController;

    @Inject
    AllocatorMetrics metrics;

    @Post(value = "/set_ready", consumes = MediaType.APPLICATION_JSON)
    public HttpResponse<String> status(@Body Map<String, String> requestBody) {
        String clusterId = requestBody.get("cluster_id");
        String nodeName = requestBody.get("node_name");

        if (clusterId == null || nodeName == null) {
            String errorMessage = "Required data is not specified";
            handleError(clusterId, nodeName, errorMessage);
            return HttpResponse.status(HttpStatus.BAD_REQUEST);
        }

        try {
            nodeController.addLabels(clusterId, nodeName, Map.of("lzy.ai/node-ready-to-use", "true"));
        } catch (IllegalArgumentException e) {
            handleError(clusterId, nodeName, e.getMessage());
            return HttpResponse.status(HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            handleError(clusterId, nodeName, e.getMessage());
            return HttpResponse.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }


        LOG.error("Node set ready; clusterId: {}, nodeName: {}", clusterId, nodeName);
        return HttpResponse.ok("OK");
    }

    private void handleError(String clusterId, String nodeName, String errorMessage) {
        LOG.error("Failed to set node ready (clusterId: {}, nodeName: {}): {}",
            clusterId, nodeName, errorMessage);
        metrics.nodeReadinessError.inc();
    }

}
