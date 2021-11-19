package ru.yandex.qe.s3.ttl;
import javax.ws.rs.core.MediaType;
import java.nio.charset.StandardCharsets;

public abstract class MediaTypeConstants {

    public final static String APPLICATION_JSON_WITH_UTF = MediaType.APPLICATION_JSON + "; " + MediaType.CHARSET_PARAMETER + "=" + "UTF-8";
    public static final String APPLICATION_FORM_URLENCODED_WITH_UTF = MediaType.APPLICATION_FORM_URLENCODED + "; " + MediaType.CHARSET_PARAMETER + "=" + "UTF-8";

    public final static MediaType APPLICATION_JSON_WITH_UTF_TYPE = new MediaType(
            MediaType.APPLICATION_JSON_TYPE.getType(),
            MediaType.APPLICATION_JSON_TYPE.getSubtype(),
            StandardCharsets.UTF_8.name()
    );

    public static final MediaType APPLICATION_FORM_URLENCODED_WITH_UTF_TYPE = new MediaType(
            MediaType.APPLICATION_FORM_URLENCODED_TYPE.getType(),
            MediaType.APPLICATION_FORM_URLENCODED_TYPE.getSubtype(),
            StandardCharsets.UTF_8.name()
    );
}
