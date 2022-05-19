package ru.yandex.cloud.ml.platform.lzy.kharon;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static ru.yandex.cloud.ml.platform.lzy.kharon.TerminalSession.SESSION_ID_KEY;

public class UriResolver {
    private final URI externalAddress;
    private final URI servantFsProxyAddress;

    public UriResolver(URI externalAddress, URI servantFsProxyAddress) {
        this.externalAddress = externalAddress;
        this.servantFsProxyAddress = servantFsProxyAddress;
    }

    public static String parseTidFromSlotUri(URI slotUri) {
        return Path.of(slotUri.getPath()).getName(0).toString();
    }

    public static String parseSlotNameFromSlotUri(URI slotUri) {
        Path path = Path.of(slotUri.getPath());
        return Path.of("/", path.subpath(1, path.getNameCount()).toString()).toString();
    }

    public static String parseSessionIdFromSlotUri(URI slotUri) {
        for (String queryPart : slotUri.getQuery().split("\\?")) {
            final int equalPos = queryPart.indexOf('=');
            final String key = queryPart.substring(0, equalPos);
            final String value = queryPart.substring(equalPos + 1);
            if (key.equals(TerminalSession.SESSION_ID_KEY)) {
                return value;
            }
        }
        throw new IllegalStateException("Failed to parse sessionId from uri " + slotUri);
    }

    public URI appendWithSessionId(URI slotUri, String sessionId) throws URISyntaxException {
        return new URI(
            slotUri.getScheme(),
            null,
            servantFsProxyAddress.getHost(),
            servantFsProxyAddress.getPort(),
            slotUri.getPath(),
            SESSION_ID_KEY + "=" + sessionId.toString(),
            null
        );
    }

    public URI convertToKharonWithSlotUri(URI servantUri) throws URISyntaxException {
        return new URIBuilder()
                .setScheme("kharon")
                .setHost(externalAddress.getHost())
                .setPort(externalAddress.getPort())
                .addParameter("slot_uri", servantUri.toString())
                .build();
    }

    @Nullable
    public static URI parseSlotUri(URI servantUri) {
        return URLEncodedUtils.parse(servantUri, StandardCharsets.UTF_8)
                .stream()
                .filter(t -> t.getName().equals("slot_uri"))
                .findFirst()
                .map(NameValuePair::getValue)
                .map(URI::create)
                .orElse(null);
    }
}
