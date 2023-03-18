package ai.lzy.tunnel.service;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

@Singleton
@Requires(notEnv = "test")
public class LinuxTunnelManager implements TunnelManager {
    private static final Logger LOG = LogManager.getLogger(LinuxTunnelManager.class);

    public static final String CREATE_TUNNEL_PATH = "/app/resources/scripts/create-tunnel.sh";
    public static final String DELETE_TUNNEL_PATH = "/app/resources/scripts/delete-tunnel.sh";

    @Override
    public void createTunnel(String remoteV6Address, String podV4Address, String podsCIDR, int tunnelIndex) {
        startProcessAndWait(CREATE_TUNNEL_PATH, remoteV6Address, podV4Address, podsCIDR, String.valueOf(tunnelIndex));
    }

    @Override
    public void destroyTunnel() {
        startProcessAndWait(DELETE_TUNNEL_PATH);
    }

    private static void startProcessAndWait(String... cmd) {
        if (cmd.length == 0) {
            throw new IllegalStateException("Cannot create process without command");
        }
        try {
            var process = new ProcessBuilder(cmd)
                    .redirectErrorStream(true)
                    .start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String s;
            while ((s = reader.readLine()) != null) {
                LOG.info(s);
            }

            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                LOG.warn("Still waiting after 10 seconds to process {} to finish. Destroying...", process.pid());
                process.destroy();
            }
            LOG.info("Process finished with code: {}", process.exitValue());
        } catch (IOException e) {
            LOG.error("Error while reading execution", e);
        } catch (InterruptedException e) {
            LOG.error("Process was interrupted", e);
        }
    }
}
