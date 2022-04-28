package ru.yandex.cloud.ml.platform.lzy.kharon;

import static ru.yandex.cloud.ml.platform.lzy.kharon.TerminalSession.SESSION_ID_KEY;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

public class UriResolver {
    private URI kharonServantProxyAddress;

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

    public URI resolveSlotUri(URI slotUri) throws URISyntaxException {

    }
}
