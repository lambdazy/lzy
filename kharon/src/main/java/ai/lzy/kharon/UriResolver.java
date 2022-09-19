package ai.lzy.kharon;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;

public class UriResolver {
    private final URI externalAddress;
    private final URI servantFsProxyAddress;

    public UriResolver(URI externalAddress, URI servantFsProxyAddress) {
        this.externalAddress = externalAddress;
        this.servantFsProxyAddress = servantFsProxyAddress;
    }

    public URI convertToServantFsProxyUri(URI slotUri) throws URISyntaxException {
        return new URI(
            slotUri.getScheme(),
            null,
            servantFsProxyAddress.getHost(),
            servantFsProxyAddress.getPort(),
            slotUri.getPath(),
            null,
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
