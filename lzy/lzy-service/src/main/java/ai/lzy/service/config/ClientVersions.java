package ai.lzy.service.config;

import io.grpc.Status;
import io.grpc.StatusException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;

public class ClientVersions {
    private static final Logger LOG = LogManager.getLogger(ClientVersions.class);
    private static final Map<String, ClientVersionsDescription> versions = Map.of(
        "pylzy", new ClientVersionsDescription(
            /* minimal version */ new SemanticVersion(1, 12, 0),
            Set.of()
        )
    );

    public static boolean isSupported(String clientVersion) throws StatusException {
        var res = clientVersion.split("=");  // Requires format <client_name>=<version>
        if (res.length != 2) {
            LOG.error("Got client request with version {}, witch is in incorrect format", clientVersion);
            throw Status.INVALID_ARGUMENT
                .withDescription("Got client request incorrect version format")
                .asException();
        }
        var clientName = res[0];
        var version = res[1];

        var versionParts = version.split("\\.");  // Required format of version <major>.<minor>.<path>

        if (versionParts.length != 3) {
            LOG.error("Got client request with version {}, witch is in incorrect format", clientVersion);
            throw Status.INVALID_ARGUMENT
                .withDescription("Got client request incorrect version format")
                .asException();
        }
        final int maj;
        final int min;
        final int path;

        try {
            maj = Integer.parseInt(versionParts[0]);
            min = Integer.parseInt(versionParts[1]);
            path = Integer.parseInt(versionParts[2]);
        } catch (NumberFormatException e) {
            LOG.error("Version {} contains not integer parts: ", clientVersion, e);
            throw Status.INVALID_ARGUMENT
                .withDescription("Got client request incorrect version format")
                .asException();
        }

        var ver = new SemanticVersion(maj, min, path);

        var desc = versions.get(clientName);

        if (desc == null) {
            LOG.error("Client with version {} not found", clientVersion);
            throw Status.FAILED_PRECONDITION
                .withDescription("Client with this name is unsupported")
                .asException();
        }

        if (ver.smallerThen(desc.minimalVersion)) {
            LOG.warn("Got client request with unsupported version {}, minimal supported version is {}",
                clientVersion, desc.minimalVersion);
            return false;
        }

        if (desc.blacklist.contains(ver)) {
            LOG.warn("Got client request with blacklisted version {}", clientVersion);
            return false;
        }

        return true;
    }

    private record ClientVersionsDescription(
        SemanticVersion minimalVersion,
        Set<SemanticVersion> blacklist
    ) { }
}
