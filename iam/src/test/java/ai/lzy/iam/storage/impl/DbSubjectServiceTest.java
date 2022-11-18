package ai.lzy.iam.storage.impl;

import ai.lzy.iam.BaseSubjectServiceApiTest;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.storage.db.IamDataSource;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthInternalException;
import ai.lzy.util.auth.exceptions.AuthNotFoundException;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

public class DbSubjectServiceTest extends BaseSubjectServiceApiTest {
    public static final Logger LOG = LogManager.getLogger(DbSubjectServiceTest.class);

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
    public void addToSubjectMultipleSameCredentialsIsOkay() {
        var dima = createSubject("Dima", SubjectType.USER);

        var credentialsName1 = "Scotty-secure";
        var credentialsName2 = "New-chapter";

        addCredentials(dima, credentialsName1);
        addCredentials(dima, credentialsName2);
        addCredentials(dima, credentialsName1);
        addCredentials(dima, credentialsName2);
        addCredentials(dima, credentialsName1);

        var credentials1 = credentials(dima, credentialsName1);
        var credentials2 = credentials(dima, credentialsName2);

        assertEquals(credentialsName1, credentials1.name());
        assertEquals("Value", credentials1.value());
        assertEquals(CredentialsType.PUBLIC_KEY, credentials1.type());
        assertEquals(credentialsName2, credentials2.name());
        assertEquals("Value", credentials2.value());
        assertEquals(CredentialsType.PUBLIC_KEY, credentials2.type());

        removeCredentials(dima, credentialsName1);
        credentials(dima, credentialsName2);
        try {
            credentials(dima, credentialsName1);
            fail();
        } catch (NoSuchElementException e) {
            LOG.info("Valid exception {}", e.getMessage());
        } catch (AuthNotFoundException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }

        removeCredentials(dima, credentialsName2);
        try {
            credentials(dima, credentialsName2);
            fail();
        } catch (NoSuchElementException e) {
            LOG.info("Valid exception {}", e.getMessage());
        } catch (AuthNotFoundException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }
    }

    @Ignore
    @Test
    public void createSubjectWithDuplicatedCredentialsIsOkay() {
        var credentialName1 = "work-macbook";
        var credentialName2 = "virtual-vm";
        var credentials1 = new SubjectCredentials(credentialName1, "Value", CredentialsType.PUBLIC_KEY);
        var credentials2 = new SubjectCredentials(credentialName2, "Value", CredentialsType.OTT,
            Instant.now().plus(Duration.ofDays(30)));

        var anton = subjectService.createSubject(AuthProvider.GITHUB, "Anton", SubjectType.USER, List.of(
            credentials1, credentials2, credentials1
        ));

        var actualCredentials2 = credentials(anton, credentialName2);
        var actualCredentials1 = credentials(anton, credentialName1);

        assertEquals(anton.id(), subject(anton.id()).id());
        assertEquals(credentialName1, actualCredentials1.name());
        assertEquals(credentialName2, actualCredentials2.name());

        removeSubject(anton);
        try {
            subject(anton.id());
            credentials(anton, credentialName1);
            credentials(anton, credentialName2);
        } catch (NoSuchElementException e) {
            LOG.info("Valid exception {}", e.getMessage());
        } catch (AuthNotFoundException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }
    }

    @Test
    public void addCredentialsWithSameNameButDifferentPropertiesIsError() {
        var dima = createSubject("Dima", SubjectType.USER);

        var credentialsName1 = "Scotty-secure";
        var credentialsName2 = "New-chapter";

        addCredentials(dima, credentialsName1);
        addCredentials(dima, credentialsName2);

        var credentials1 = credentials(dima, credentialsName1);
        var credentials2 = credentials(dima, credentialsName2);

        assertEquals(credentialsName1, credentials1.name());
        assertEquals("Value", credentials1.value());
        assertEquals(CredentialsType.PUBLIC_KEY, credentials1.type());
        assertEquals(credentialsName2, credentials2.name());
        assertEquals("Value", credentials2.value());
        assertEquals(CredentialsType.PUBLIC_KEY, credentials2.type());

        var credentials1ReplicaWithOtherValueAndType = new SubjectCredentials(credentialsName1, "--vALuE--",
            CredentialsType.COOKIE);
        var credentials2ReplicaWithOtherTypeAndTtl = new SubjectCredentials(credentialsName2, "Value",
            CredentialsType.OTT, Instant.now().plus(Duration.ofDays(30)));

        try {
            subjectService.addCredentials(dima, credentials1ReplicaWithOtherValueAndType);
            fail();
        } catch (AuthException e) {
            assertSame(IllegalArgumentException.class, e.getCause().getClass());
        }

        try {
            subjectService.addCredentials(dima, credentials2ReplicaWithOtherTypeAndTtl);
            fail();
        } catch (AuthException e) {
            assertSame(IllegalArgumentException.class, e.getCause().getClass());
        }
    }

    @Test
    public void createSubjectWithCredentialsWithSameNameButDifferentPropertiesIsError() {
        var credentialName1 = "work-macbook";
        var credentialName2 = "virtual-vm";
        var credentials1 = new SubjectCredentials(credentialName1, "Value", CredentialsType.PUBLIC_KEY);
        var credentials2 = new SubjectCredentials(credentialName2, "Value", CredentialsType.OTT,
            Instant.now().plus(Duration.ofDays(30)));
        var credentials2ReplicaWithOtherTtl = new SubjectCredentials(credentialName2, "Value", CredentialsType.OTT,
            Instant.now().plus(Duration.ofMinutes(30)));

        try {
            subjectService.createSubject(AuthProvider.GITHUB, "Anton", SubjectType.USER, List.of(
                credentials1, credentials2, credentials2ReplicaWithOtherTtl
            ));
            fail();
        } catch (AuthInternalException e) {
            //
        }
    }

    @Override
    protected Subject subject(String id) {
        return subjectService.subject(id);
    }

    @Override
    protected Subject createSubject(String name, SubjectType subjectType) {
        return subjectService.createSubject(AuthProvider.GITHUB, name, subjectType, List.of());
    }

    @Override
    protected void removeSubject(Subject subject) {
        subjectService.removeSubject(subject);
    }

    @Override
    protected SubjectCredentials credentials(Subject subject, String name) throws NoSuchElementException {
        return subjectService.credentials(subject, name);
    }

    @Override
    protected void addCredentials(Subject subject, String name) {
        subjectService.addCredentials(subject, new SubjectCredentials(name, "Value", CredentialsType.PUBLIC_KEY));
    }

    @Override
    protected void removeCredentials(Subject subject, String name) {
        subjectService.removeCredentials(subject, name);
    }
}
