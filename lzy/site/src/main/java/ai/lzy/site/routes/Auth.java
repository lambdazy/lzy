package ai.lzy.site.routes;

import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.site.ServiceConfig;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.auth.exceptions.AuthUniqueViolationException;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientException;
import io.micronaut.http.cookie.Cookie;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.http.server.util.HttpHostResolver;
import io.micronaut.http.uri.UriBuilder;
import io.micronaut.web.router.RouteBuilder;
import jakarta.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import static ai.lzy.site.ServiceConfig.GithubCredentials;

@Controller("auth")
public class Auth {
    private static final Logger LOG = LogManager.getLogger(Auth.class);

    @Inject
    ObjectMapper objectMapper;

    @Inject
    GithubCredentials githubCredentials;

    @Inject
    SubjectServiceGrpcClient subjectServiceClient;

    @Inject
    private HttpHostResolver httpHostResolver;

    @Inject
    private RouteBuilder.UriNamingStrategy uriNamingStrategy;

    @Inject
    private ServiceConfig config;

    @Client(value = "${site.github.address}")
    @Inject
    private HttpClient githubClient;

    @Client("${site.github.api-address}")
    @Inject
    private HttpClient githubApiClient;

    @Get("login_url{?authType,siteSignInUrl}")
    public HttpResponse<LoginUrlResponse> loginUrl(
        @QueryValue AuthType authType, @QueryValue String siteSignInUrl, HttpRequest<?> httpRequest
    ) throws URISyntaxException
    {
        if (authType.equals(AuthType.GITHUB)) {
            final String loginUrl = UriBuilder.of(new URI("https://github.com/login/oauth/authorize"))
                .queryParam("client_id", githubCredentials.getClientId())
                .queryParam("state", siteSignInUrl)
                .queryParam("redirect_uri", config.getHostname()
                    + uriNamingStrategy.resolveUri(Auth.class) + "/code/github")
                .build().toString();
            return HttpResponse.ok(new LoginUrlResponse(loginUrl));
        }
        throw new HttpStatusException(HttpStatus.BAD_REQUEST, "Unknown auth type " + "loginUrlRequest");
    }

    @Get("/code/github{?code,state}")
    public HttpResponse<Object> acceptGithubCode(@QueryValue String code, @QueryValue String state) {
        final var tokenRequest = new GithubAccessTokenRequest(code,
            githubCredentials.getClientId(), githubCredentials.getClientSecret());
        LOG.info("Checking github code: " + code);

        final var response = githubClient.toBlocking().exchange(
            HttpRequest.POST("/login/oauth/access_token", tokenRequest)
                .accept(MediaType.APPLICATION_JSON_TYPE),
            GithubAccessTokenResponse.class
        );

        if (response.getStatus() != HttpStatus.OK) {
            LOG.error("Code request status: " + response.getStatus());
            throw new HttpStatusException(HttpStatus.FORBIDDEN, "Bad code");
        }

        final var body = response.getBody().orElseThrow();
        if (body.accessToken() == null) {
            LOG.error("Access token in body is null");
            throw new HttpStatusException(HttpStatus.FORBIDDEN, "Bad code");
        }

        LOG.info("Code is OK; Getting user");
        final String userName;
        try {
            var result = githubApiClient.toBlocking().exchange(
                HttpRequest.GET("/user")
                    .header("Authorization", "token " + body.accessToken())
                    .header("User-Agent", "request")
                    .accept(MediaType.APPLICATION_JSON_TYPE),
                GitHubGetUserResponse.class
            );
            if (result.getStatus() != HttpStatus.OK) {
                throw new HttpStatusException(HttpStatus.FORBIDDEN, "Bad code");
            }
            userName = result.getBody().orElseThrow().login();
        } catch (HttpClientException e) {
            LOG.error("Failed to get user, cause:", e);
            throw new HttpStatusException(HttpStatus.FORBIDDEN, "Bad code");
        }

        final Duration maxAge = Duration.ofDays(30);

        final RsaUtils.RsaKeys keys;
        try {
            keys = RsaUtils.generateRsaKeys();
        } catch (IOException | InterruptedException e) {
            LOG.error("Cannot generate rsa keys: ", e);
            throw new RuntimeException(e);
        }

        var keyName = String.format("Site session %s", new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()));

        var cred = SubjectCredentials.publicKey(keyName, keys.publicKey(), maxAge);

        final String token;
        try {
            token = JwtUtils.buildJWT(userName, AuthProvider.GITHUB.name(),
                Date.from(Instant.now()), Date.from(Instant.now().plus(maxAge)),
                new StringReader(keys.privateKey()), cred.name());
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            LOG.error("Cannot generate jwt token: ", e);
            throw new RuntimeException(e);
        }

        Subject subject;

        try {
            subject = subjectServiceClient.createSubject(AuthProvider.GITHUB, userName, SubjectType.USER, cred);
        } catch (AuthUniqueViolationException e) {
            LOG.info("Logging in already created user {}", userName);
            subject = subjectServiceClient.findSubject(AuthProvider.GITHUB, userName, SubjectType.USER);
            if (subject == null) {
                LOG.error("Cannot get subject <{}>, but its already exists", userName);
                throw new RuntimeException("Internal error while getting subject");
            }
            subjectServiceClient.addCredentials(subject, cred);
        }

        final URI siteSignInUrl = URI.create(state);
        return HttpResponse.redirect(siteSignInUrl)
            .cookie(Cookie.of("userId", userName).path("/"))
            .cookie(Cookie.of("userSubjectId", subject.id()).path("/"))
            .cookie(Cookie.of("sessionId", token).path("/"));
    }

    public enum AuthType {
        GITHUB("github");

        public final String name;

        AuthType(String name) {
            this.name = name;
        }

        public String toString() {
            return name;
        }
    }

    @Introspected
    public record LoginUrlResponse(
        String url
    )
    {
    }

    @Introspected
    public record GithubAccessTokenRequest(
        String code,
        @JsonProperty(value = "client_id")
        String clientId,
        @JsonProperty(value = "client_secret")
        String clientSecret
    )
    {
    }


    @Introspected
    public record GithubAccessTokenResponse(
        @JsonProperty(value = "access_token")
        String accessToken,
        String scope,
        @JsonProperty(value = "token_type")
        String tokenType
    )
    {
    }

    @Introspected
    public record GitHubGetUserResponse(
        String login,
        String id
    )
    {
    }
}
