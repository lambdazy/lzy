package ai.lzy.iam.storage.impl;

import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.storage.db.IamDataSource;
import ai.lzy.model.db.test.DatabaseTestUtils;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;

public class CredentialsTtlTest {
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext ctx;
    private DbSubjectService subjectService;
    private IamDataSource storage;

    @Before
    public void setUp() {
        ctx = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("iam", db.getConnectionInfo()));

        storage = ctx.getBean(IamDataSource.class);
        subjectService = ctx.getBean(DbSubjectService.class);
    }

    @After
    public void tearDown() {
        DatabaseTestUtils.cleanup(storage);
        ctx.stop();
    }

    @Test
    public void publicKeyNotRequiresTtl() {
        new SubjectCredentials("name", "value", CredentialsType.PUBLIC_KEY, null);
    }

    @Test
    public void ottRequiresTtl() {
        try {
            new SubjectCredentials("name", "value", CredentialsType.OTT, null);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("OTT must be with ttl", e.getMessage());
        }
    }

    @Test
    public void test() {
        var expired = Instant.now().minus(1, ChronoUnit.DAYS);
        var actual = Instant.now().plus(1, ChronoUnit.DAYS);

        var creds1 = new SubjectCredentials("1", "1", CredentialsType.PUBLIC_KEY);
        var creds2 = new SubjectCredentials("2", "2", CredentialsType.PUBLIC_KEY, actual);
        var creds3 = new SubjectCredentials("3", "3", CredentialsType.PUBLIC_KEY, expired);
        var creds4 = new SubjectCredentials("4", "4", CredentialsType.OTT, actual);
        var creds5 = new SubjectCredentials("5", "5", CredentialsType.OTT, expired);

        var subject = subjectService.createSubject(AuthProvider.GITHUB, "subject", SubjectType.USER, List.of(
            creds1, creds2, creds3, creds4, creds5));

        var creds = subjectService.listCredentials(subject);
        Assert.assertEquals(3, creds.size());

        creds = creds.stream().sorted(Comparator.comparing(SubjectCredentials::name)).toList();
        Assert.assertEquals(creds1, creds.get(0));
        Assert.assertEquals(creds2, creds.get(1));
        Assert.assertEquals(creds4, creds.get(2));
    }
}
