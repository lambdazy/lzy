package ru.yandex.cloud.ml.platform.lzy.kharon;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;

import static ru.yandex.cloud.ml.platform.lzy.kharon.TerminalSession.SESSION_ID_KEY;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;

public class UriResolver {
    private final URI kharonServantProxyAddress;
    private final URI kharonAddress;

    public UriResolver(URI kharonServantProxyAddress, URI kharonAddress) {
        this.kharonServantProxyAddress = kharonServantProxyAddress;
        this.kharonAddress = kharonAddress;
    }

    public static String parseTidFromSlotUri(URI slotUri) {
        return Path.of(slotUri.getPath()).getName(0).toString();
    }

    public static String parseSlotNameFromSlotUri(URI slotUri) {
        Path path = Path.of(slotUri.getPath());
        return Path.of("/", path.subpath(1, path.getNameCount()).toString()).toString();
    }

    public static UUID parseSessionIdFromSlotUri(URI slotUri) {
        for (String queryPart : slotUri.getQuery().split("\\?")) {
            final int equalPos = queryPart.indexOf('=');
            final String key = queryPart.substring(0, equalPos);
            final String value = queryPart.substring(equalPos + 1);
            if (key.equals(TerminalSession.SESSION_ID_KEY)) {
                return UUID.fromString(value);
            }
        }
        throw new IllegalStateException("Failed to parse sessionId from uri " + slotUri);
    }

    public URI convertSlotUri(URI slotUri, UUID sessionId) throws URISyntaxException {
        return new URI(
            slotUri.getScheme(),
            null,
            kharonServantProxyAddress.getHost(),
            kharonServantProxyAddress.getPort(),
            slotUri.getPath(),
            SESSION_ID_KEY + "=" + sessionId.toString(),
            null
        );
    }

    public URI convertServantUri(URI slotUri) throws URISyntaxException {
        return new URIBuilder()
                .setScheme("kharon")
                .setHost(kharonAddress.getHost())
                .setPort(kharonAddress.getPort())
                .addParameter("servant_uri", slotUri.toString())
                .build();
    }

    public static Optional<URI> resolveSlotUri(URI slotUri) {
        return URLEncodedUtils.parse(slotUri, StandardCharsets.UTF_8)
                .stream()
                .filter(t -> t.getName().equals("servant_uri"))
                .findFirst()
                .map(NameValuePair::getValue)
                .map(URI::create);
    }
}
