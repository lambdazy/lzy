package ai.lzy.site.routes;

import io.micronaut.http.HttpResponse;
import org.junit.Assert;

import java.util.stream.Collectors;

public class Utils {
    public record ParsedCookies(
        String userId,
        String userSubjectId,
        String sessionId)
    { }

    public static ParsedCookies parseCookiesFromHeaders(HttpResponse<?> response) {
        Assert.assertNotNull(response.getHeaders());
        final var cookies = response.getHeaders().getAll("set-cookie").stream()
                .map(s -> s.split("=", 2))
                .collect(Collectors.toMap(s -> s[0], s -> s[1]));
        Assert.assertTrue(cookies.containsKey("userId"));
        Assert.assertTrue(cookies.containsKey("userSubjectId"));
        Assert.assertTrue(cookies.containsKey("sessionId"));

        final String sessionId = cookies.get("sessionId").split(";")[0];
        return new ParsedCookies(
            cookies.get("userId"),
            cookies.get("userSubjectId"),
            sessionId
        );
    }
}
