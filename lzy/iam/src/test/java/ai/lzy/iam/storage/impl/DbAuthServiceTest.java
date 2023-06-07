package ai.lzy.iam.storage.impl;

import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.storage.db.IamDataSource;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.util.auth.credentials.CredentialsUtils;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.exceptions.AuthPermissionDeniedException;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.Date;
import java.util.List;

public class DbAuthServiceTest {
    public static final Logger LOG = LogManager.getLogger(DbAuthServiceTest.class);

    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private static final String PUBLIC_PEM1 =
            """
                    -----BEGIN PUBLIC KEY-----
                    MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4/BSBCcNGUYcogQbSbHy
                    TZwctKRd50FBGLD7aq1nRUscp7lP6Aqymd7cb70OtbBXMEOdagq0KWczIZqWWWZy
                    wyCTe0lWN1Z2d5YxYk5aj1D+IipkPEqN2Y0CIP+dPCRCGgOBtcnGtBzWtVCDGl3t
                    a00cVl91lOVuBiz5l0d7h6vQqEB+sNI46AhKSlEG9wIsL1LnKD9XG5pMF+r5K5nM
                    F7IirVQMFR/zXNSBCDqgBJ0j7iEnWQWv36KxTWFxXseVDziva6Ph5IMX6krHHST/
                    tYzmnNUyrmjV9ClCRkYBBCouEKGncKBcsS3HWJqvX3K6Ovc+B9CKOFXbOmraAik/
                    +wIDAQAB
                    -----END PUBLIC KEY-----
                    """;

    private static final String PUBLIC_PEM2 =
            """
                    -----BEGIN PUBLIC KEY-----
                    MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6fysdbbtTN42hc9A+HYV
                    kfnWg7QtKmeJsbY1j6PL4fvydhMljLIVqEX7jxVumZaXJCX0GuJF9M9mezT8ojk3
                    nr6kXtcqAfQ5KKK2Sb1ijll3EsWoQxePDQAs76PvMFrgjlzdXiR0bpjp2orMY9df
                    NbkkLeO09LhcWEUeq7wvQKBcYV8yWJ5ZedNPSINLrLSiBItq9EASocIsX3q7PtGp
                    ggbEHSR65rqcK741tGlSwxyd4Pl7mkxJzrD8HmYVz5LKfmdBRa9G+9bNnIYjcehz
                    q6WHAcLA90AjEcVge6ygPSPs99x6DNinQEzpCQ5KusDvOcyIx3KA4eK36RdRiDBs
                    bwIDAQAB
                    -----END PUBLIC KEY-----
                    """;

    private static final String PRIVATE_PEM1 =
            """
                    -----BEGIN RSA PRIVATE KEY-----
                    MIIEpAIBAAKCAQEAwu+i1dwoKyDMOj/0CG8nyp/Se62VGh+n9kPjDcEMYlfRXiRe
                    lz6GJYcER69qs92IkUHKvDFKshatrvVs4FnLyd5OpttTyxaYFLwVavLx/YVi5x1P
                    swLPdMUTkKR0iMWp3bS5sbO0hnmTeHpz/tNZ/PbWvLZGt3Sxr+S1qwo2mLTbHq6x
                    Ld2/bIMyFqZtTp71T86R++kP6cNyEpYdKaNyjCazPfLiDdsuL64S0NIuylcdrgtH
                    F7TYZdGHKsRSmY+VkRN6DNancskpzVAvclJJQlKQFsAQHPAEiilH44unrwTcAEfU
                    RPClQn5IthTM6ytpLn7jF31bRK+efKtby7AGnQIDAQABAoIBAQCRqVUI7uCJEZHp
                    uN4V61FVhEVYq2VtxtfQGfwwy98AIVTOPuj7pPnCUvhsxHQ8AL7Ko9nk9AQ3vOB9
                    iuCXaCHyLw6geeVMLQ7o934dk3olkaVMw0dJoUD5pUWwYGK/zMvYYfIcCgdMMM50
                    STQh5zYn4x6klqOS9DzODV1+eDxBsBwmYnQIaetll+YpxHRT5sseqByY85VhXRTv
                    EpxGBMSCggC8z5fSEQeXBVYkmqLhqbP+u2/tU4tVae1vwAPbiVv47EvMaNtdW1Wa
                    JIlY/mDormG9f6vG0ezlcCl8iu21yFgvliRrVdA6/J2BN9GnwE4je+GIWpgClaR1
                    RY6+lTF1AoGBAO8sz5CnKECWPFNOdFASz6cYBUCqZL35ClKwaGNB7nYdnSgpOyTA
                    JW7tTSACZUWq+QGULMQIfTKrR11lsIVPjqQTC5iPBChesBiIJ0zmTjASxIJJ+3zr
                    PdvRutPEjJHdBZt7//UdlimjVF3Yt6N0TvVtMzcSJFF4PJ07UQdSTcxnAoGBANCm
                    JZpoc7O7dTFxO08br4bl890WhyLRtJljp7nJbGHzNJgBJi8+6VRYMEYI9w61ER6l
                    BR084LrpOyop4OR8eDqZPSwZ4hNmTaGLMrnWQ8qxetBvE7QWt8poXubk68ie8GE0
                    hZP72n+MdymyJqjrlotVIwRW9c5u6fVWO1M5WfJbAoGACU9OOQw4peLzMC7ymhdR
                    W+i0c6LuTvK9syBIv+xWEuTuNBz+v1x3WI0GHoPZW0/fZ29UGsFV1j4ShhEqQNYq
                    8DoJjoOqnsOoyRuro/OnAXoJiiTFFES34LGWOx4AdsEKsdWuzeS77pz78Lc51rP1
                    StpYTwF2xnEOsvQXIFjUzGsCgYEAynQc0pl9gy1mxqXPXbBIfgWMvb8JOxDuQ1P+
                    QHigkN5y7vdWfMt3jh7QIHS8fOnWhbyrnLYgfVynyv69uBbKdlmQkMVAp4BB3Xj6
                    rHWqa/gQakUNglX02hKx2yrPWmhWaIuU/YWIevDqA2xYtNl7xxDCHIjglADtRN/6
                    SoPAsjcCgYA48zjvjqQwr91pwzMWX2QhIvViXOYX2MWHjJd+lomkQmtTcr1Nv6rT
                    d75eBDrDZh1EPSP3yloQunsn1X9TIFmPBbOKFpGVxT1tW3t1ZZM14GtR0lQOef+e
                    fsP8dos5sZsDQU/rPAHBqburR5GTrn61hAAsDaHChwNTr1iT18pt/w==
                    -----END RSA PRIVATE KEY-----
                    """;

    private static final String PRIVATE_PEM2 =
            """
                    -----BEGIN RSA PRIVATE KEY-----
                    MIIEpAIBAAKCAQEA6fysdbbtTN42hc9A+HYVkfnWg7QtKmeJsbY1j6PL4fvydhMl
                    jLIVqEX7jxVumZaXJCX0GuJF9M9mezT8ojk3nr6kXtcqAfQ5KKK2Sb1ijll3EsWo
                    QxePDQAs76PvMFrgjlzdXiR0bpjp2orMY9dfNbkkLeO09LhcWEUeq7wvQKBcYV8y
                    WJ5ZedNPSINLrLSiBItq9EASocIsX3q7PtGpggbEHSR65rqcK741tGlSwxyd4Pl7
                    mkxJzrD8HmYVz5LKfmdBRa9G+9bNnIYjcehzq6WHAcLA90AjEcVge6ygPSPs99x6
                    DNinQEzpCQ5KusDvOcyIx3KA4eK36RdRiDBsbwIDAQABAoIBAFa4F6608jPX83sa
                    Oekb0pi8cJ11THv3zZd4gVdQDIMfnlfWdsczRUWNUlNQTSJNJoz2KAdCr0yxBTlK
                    hQsWi5+g5khkFCSPQBPoYgjoULuTOsdRTDA5bgISe5UBO+e+9pSspDp85k4LDDi7
                    0k56hsXhbSA40VsVbNwmGdzqLNUVJFLHV48eLRYovgCzDTtJBydmc/XtbE7bpEWq
                    z2JZU7YgIQK2lTexIRUTLIaFzj5IMIrVGOjYh1d7ErxXegZHrequi2c8ul8oUnwT
                    NbctJmCTBSe++5L7ZP06DMZKOZHI6IGLooFBVmCZlRQUj/+AV0lwreeniUTbbPWo
                    FppaP4ECgYEA+gPNEKqKArP6Wu+6qlhBTgF0s7YSXxVS3rJ8WixnWR9ZQSNkTZGW
                    jAixu+bHetpnpz2cHYojlqR7kzZdvy2Tg7x7ahFbygc6vBMyPFHrnwucedQ7AdBs
                    FM5NyGQK6KNxwL5MF9hVux4FrqAGCvnSbPwhDv4a1ZyVUVgIC28Nbq8CgYEA75al
                    qH01nFz/9dwhJ39SzvQodf9xHyVbRspAS1OmGvnMZEOw+r/aKSKSXdAYUr0D2vXl
                    DxQuOIDPNBu/tzYKqEHGK7F+rHAztSlwuIDpnw3zQhROo5HL7gwqKJa1oNWcA9+4
                    wBV2hV8647a+3tcHq1vYcfkX6jW/t3q8aFZSTkECgYEA2gremR5iZqEYQp64qT93
                    FNToNqMfupUaROZc1TfMmklgyhJXs9648T/T4hAPAPHhXFW7BXgoOYUR0P2lHMpe
                    0JFdANBKwRM1Ajmroje/ymGSAh45qAdhe3PBGndFnEaPOo28Rz+A5UP1qKofGwtt
                    nWb2XeD2/j0lbF/eBDrB+DkCgYEAklkTF7hj5v9n2mZ8WgQMMR4zGODP2JaZCsTA
                    QUL3U8MCdrxifshyGm5juapDMUcD89v/7xYEpb8I0mugz+jS2bRTuJzTI8Hl0+Nc
                    V1dnXSDIVrTQ1FlamC4WEnT8vSG+Cx+9WpfBrfZdonseXEA9Dw8rR3NAiHAMi6cN
                    Ly6LWAECgYAH2NmPcRp0SXRi2h8afLeawhdQZjHJ79ICC783CuiYz3MUVRdYy1kw
                    hUALqfnNx+gdNxorLJ0yikFxHBENIJSdXCoDAAoYk4locFj928+B76uqFcd/807i
                    yg8CTD6w6uwHXxKlLBOIgZg6Dt1iudxwuSSVY/HSmv86Bv2DmfqI8g==
                    -----END RSA PRIVATE KEY-----
                    """;

    private ApplicationContext ctx;
    private DbSubjectService subjectService;
    private IamDataSource storage;
    private DbAuthService authenticateService;

    @Before
    public void setUp() {
        ctx = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("iam", db.getConnectionInfo()));

        storage = ctx.getBean(IamDataSource.class);
        subjectService = ctx.getBean(DbSubjectService.class);
        authenticateService = ctx.getBean(DbAuthService.class);
    }

    @After
    public void tearDown() {
        storage.setOnClose(DatabaseTestUtils::cleanup);
        ctx.stop();
    }

    @Test
    public void validAuthUser() throws Exception {
        validAuth(SubjectType.USER);
    }

    @Test
    public void validAuthWorker() throws Exception {
        validAuth(SubjectType.WORKER);
    }

    public void validAuth(SubjectType subjectType) throws Exception {
        var authProvider = subjectType == SubjectType.WORKER ? AuthProvider.INTERNAL : AuthProvider.GITHUB;
        var userId = subjectService.createSubject(authProvider, "user1", subjectType, List.of(
            new SubjectCredentials("testCred", PUBLIC_PEM2, CredentialsType.PUBLIC_KEY)), "hash").id();

        final Subject user = subjectService.subject(userId);

        var jwt = JwtUtils.buildJWT("user1", authProvider.name(), Date.from(Instant.now()),
            JwtUtils.afterDays(1), CredentialsUtils.readPrivateKey(PRIVATE_PEM2));
        authenticateService.authenticate(new JwtCredentials(jwt));
    }

    @Test
    public void invalidAuthUser() throws Exception {
        invalidAuth(SubjectType.USER);
    }

    @Test
    public void invalidAuthWorker() throws Exception {
        invalidAuth(SubjectType.WORKER);
    }

    public void invalidAuth(SubjectType subjectType) throws Exception {
        var authProvider = subjectType == SubjectType.WORKER ? AuthProvider.INTERNAL : AuthProvider.GITHUB;
        var userId = subjectService.createSubject(authProvider, "user1", subjectType, List.of(
            new SubjectCredentials("testCred", PUBLIC_PEM2, CredentialsType.PUBLIC_KEY)), "hash").id();

        final Subject user = subjectService.subject(userId);

        var jwt = JwtUtils.buildJWT("user1", authProvider.name(), Date.from(Instant.now()),
            JwtUtils.afterDays(1), CredentialsUtils.readPrivateKey(PRIVATE_PEM2));
        try {
            authenticateService.authenticate(new JwtCredentials(jwt));
        } catch (AuthPermissionDeniedException e) {
            LOG.info("Valid error::{}", e.getInternalDetails());
        }
    }
}
