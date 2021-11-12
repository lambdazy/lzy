package ru.yandex.cloud.ml.platform.lzy.model.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Set;

public class FreePortFinder {
    private static final Logger LOG = LogManager.getLogger(FreePortFinder.class);
    private static final Set<Integer> usedPorts = new HashSet<>();

    public static class FailedFindEmptyPortException extends RuntimeException {
        public FailedFindEmptyPortException(String message) {
            super(message);
        }
    }

    public synchronized static int find(int min, int max) {
        LOG.info("Searching empty port range [{},{}]", min, max);
        for (int port = min; port < max; port++) {
            if (usedPorts.contains(port)) {
                continue;
            }
            try (ServerSocket ignored = new ServerSocket(port)) {
                usedPorts.add(port);
                return port;
            } catch (IOException e) {
                LOG.info("Port " + port + " already in use");
            }
        }
        throw new FailedFindEmptyPortException(String.format("No ports in range [%s,%s]", min, max));
    }
}
