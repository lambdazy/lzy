package ai.lzy.allocator.services;

import ai.lzy.allocator.alloc.AllocatorMetrics;
import ai.lzy.allocator.alloc.impl.kuber.NodeController;
import com.google.common.net.HostAndPort;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.server.util.HttpClientAddressResolver;
import jakarta.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static ai.lzy.allocator.alloc.impl.kuber.KuberLabels.NODE_READINESS_LABEL;

@Controller("/kuber_node")
public class KuberNodeService {
    private static final Logger LOG = LogManager.getLogger(KuberNodeService.class);

    private static final int NODE_DAEMONSET_PORT = 8042;

    @Inject
    NodeController nodeController;

    @Inject
    AllocatorMetrics metrics;

    @Inject
    HttpClientAddressResolver clientAddressResolver;

    Set<String> readyNodes = new HashSet<>();

    HttpClient httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofMillis(1000))
        .followRedirects(HttpClient.Redirect.NEVER)
            .build();

    @Post(value = "/set_ready", consumes = MediaType.APPLICATION_JSON)
    public HttpResponse<String> status(HttpRequest<Map<String, String>> request) {
        var requestBody = request.getBody().orElse(Map.of());
        final String clientAddress = clientAddressResolver.resolve(request);
        final String clusterId = requestBody.get("cluster_id");
        final String nodeName = requestBody.get("node_name");

        if (clusterId == null || nodeName == null) {
            String errorMessage = "Required data is not specified";
            handleError(clientAddress, clusterId, nodeName, errorMessage);
            return HttpResponse.status(HttpStatus.BAD_REQUEST);
        }

        LOG.info("Set node ready request; clientAddress: {}, clusterId: {}, nodeName: {}",
            clientAddress, clusterId, nodeName);

        if (readyNodes.contains(nodeName)) {
            String errorMessage = "Node was set ready before";
            handleError(clientAddress, clusterId, nodeName, errorMessage);
            return HttpResponse.status(HttpStatus.BAD_REQUEST);
        }

        try {
            if (clientAddress == null || !isNodeAddress(clientAddress, clusterId, nodeName)) {
                String errorMessage = "Unexpected client address";
                handleError(clientAddress, clusterId, nodeName, errorMessage);
                return HttpResponse.status(HttpStatus.BAD_REQUEST);
            }

            if (!ensureNodeIsReady(clientAddress)) {
                String errorMessage = "Node denied its own readiness";
                handleError(clientAddress, clusterId, nodeName, errorMessage);
                return HttpResponse.status(HttpStatus.BAD_REQUEST);
            }

            nodeController.addLabels(clusterId, nodeName, Map.of(NODE_READINESS_LABEL, "true"));
        } catch (IllegalArgumentException e) {
            handleError(clientAddress, clusterId, nodeName, e.getMessage());
            return HttpResponse.status(HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            handleError(clientAddress, clusterId, nodeName, e.getMessage());
            return HttpResponse.status(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        readyNodes.add(nodeName);
        LOG.info("Set node ready done; nodeName: {}", nodeName);
        return HttpResponse.ok("OK");
    }

    private boolean isNodeAddress(String address, String clusterId, String nodeName) {
        var nodeAddresses = nodeController.getNode(clusterId, nodeName).getStatus().getAddresses().stream()
            .map(addr -> {
                try {
                    return InetAddress.getByName(addr.getAddress());
                } catch (UnknownHostException e) {
                    LOG.warn("Failed to parse original node address {}", addr.getAddress());
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .toList();
        try {
            return nodeAddresses.contains(InetAddress.getByName(address));
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Failed to parse client address");
        }
    }

    private boolean ensureNodeIsReady(String host) {
        final var requestUrl = "http://" + HostAndPort.fromParts(host, NODE_DAEMONSET_PORT) + "/isReady";
        final var request = java.net.http.HttpRequest.newBuilder()
            .GET()
            .uri(URI.create(requestUrl))
            .build();

        final java.net.http.HttpResponse<String> response;
        try {
            response = httpClient.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            LOG.error("Failed to send request ({}): {}", requestUrl, e.getMessage(), e);
            return false;
        } catch (InterruptedException e) {
            LOG.error("Failed to send request ({}): interrupted", requestUrl, e);
            return false;
        }
        return response.statusCode() == HttpStatus.OK.getCode() && response.body().trim().equals("true");
    }

    private void handleError(String clientAddress, String clusterId, String nodeName, String errorMessage) {
        LOG.error("Failed to set node ready (clientAddress: {}, clusterId: {}, nodeName: {}): {}",
            clientAddress, clusterId, nodeName, errorMessage);
        metrics.nodeReadinessError.inc();
    }
}
