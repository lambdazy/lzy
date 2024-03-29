package ai.lzy.iam.storage.impl;

import ai.lzy.iam.BaseSubjectServiceApiTest;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.resources.subjects.User;
import ai.lzy.iam.storage.db.IamDataSource;
import ai.lzy.iam.utils.ProtoConverter;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.util.auth.exceptions.AuthInternalException;
import ai.lzy.util.auth.exceptions.AuthNotFoundException;
import ai.lzy.util.auth.exceptions.AuthUniqueViolationException;
import ai.lzy.v1.iam.LSS;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.lzy.iam.utils.IdempotencyUtils.md5;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
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
        storage.setOnClose(DatabaseTestUtils::cleanup);
        ctx.stop();
    }

    @Test
    public void createMultipleSameSubjectsIsOkay() {
        var credentials = List.of(
            new SubjectCredentials("key1", "val", CredentialsType.PUBLIC_KEY, Instant.now().plus(Duration.ofDays(120))),
            new SubjectCredentials("key2", "val", CredentialsType.PUBLIC_KEY, Instant.now().plus(Duration.ofDays(30)))
        );

        var dima = createSubject("Dima", SubjectType.USER, credentials);
        var anotherDima = createSubject("Dima", SubjectType.USER, credentials);

        var actualDima = subject(dima.id());

        assertEquals(dima.id(), anotherDima.id());
        assertEquals(dima.id(), actualDima.id());
        assertSame(dima.type(), actualDima.type());

        removeSubject(anotherDima.id());
        assertThrows(AuthNotFoundException.class, () -> subject(dima.id()));
    }

    @Test
    public void createMultipleSameSubjectsConcurrent() throws InterruptedException {
        final int N = 10;
        final var readyLatch = new CountDownLatch(N);
        final var doneLatch = new CountDownLatch(N);
        final var executor = Executors.newFixedThreadPool(N);
        final var subjectIds = new String[N];
        final var failed = new AtomicBoolean(false);

        for (int i = 0; i < N; ++i) {
            final int index = i;
            executor.submit(() -> {
                try {
                    readyLatch.countDown();
                    readyLatch.await();

                    var credentials = List.of(
                        new SubjectCredentials("key1", "val", CredentialsType.PUBLIC_KEY,
                            Instant.now().plus(Duration.ofDays(120))),
                        new SubjectCredentials("key2", "val", CredentialsType.PUBLIC_KEY,
                            Instant.now().plus(Duration.ofDays(30)))
                    );

                    var dima = createSubject("Dima", SubjectType.USER, credentials);

                    subjectIds[index] = dima.id();
                } catch (Exception e) {
                    failed.set(true);
                    e.printStackTrace(System.err);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        doneLatch.await();
        executor.shutdown();

        assertFalse(failed.get());
        assertFalse(subjectIds[0].isEmpty());
        assertTrue(Arrays.stream(subjectIds).allMatch(subjectId -> subjectId.equals(subjectIds[0])));
    }

    @Test
    public void createSameSubjectsWithDifferentInitCredentialsIsError() {
        var credentials = List.of(
            new SubjectCredentials("key1", "val", CredentialsType.PUBLIC_KEY, Instant.now().plus(Duration.ofDays(120))),
            new SubjectCredentials("key2", "val", CredentialsType.PUBLIC_KEY, Instant.now().plus(Duration.ofDays(30)))
        );

        var dima = createSubject("Dima", SubjectType.USER, credentials);
        var actualDima = subject(dima.id());
        assertEquals(dima.id(), actualDima.id());
        assertSame(dima.type(), actualDima.type());

        assertThrows(AuthUniqueViolationException.class, () -> createSubject("Dima", SubjectType.USER));

        removeSubject(dima.id());
        assertThrows(AuthNotFoundException.class, () -> subject(dima.id()));
    }

    @Test
    public void addToSubjectMultipleSameCredentialsIsOkay() {
        var dima = createSubject("Dima", SubjectType.USER);

        var credentialsName1 = "Scotty-secure";
        var credentialsName2 = "New-chapter";

        addCredentials(dima.id(), credentialsName1);
        addCredentials(dima.id(), credentialsName2);
        addCredentials(dima.id(), credentialsName1);
        addCredentials(dima.id(), credentialsName2);
        addCredentials(dima.id(), credentialsName1);

        var credentials1 = credentials(dima.id(), credentialsName1);
        var credentials2 = credentials(dima.id(), credentialsName2);

        assertEquals(credentialsName1, credentials1.name());
        assertEquals("Value", credentials1.value());
        assertEquals(CredentialsType.PUBLIC_KEY, credentials1.type());
        assertEquals(credentialsName2, credentials2.name());
        assertEquals("Value", credentials2.value());
        assertEquals(CredentialsType.PUBLIC_KEY, credentials2.type());

        removeCredentials(dima.id(), credentialsName1);
        credentials(dima.id(), credentialsName2);

        assertThrows(AuthNotFoundException.class, () -> credentials(dima.id(), credentialsName1));

        removeCredentials(dima.id(), credentialsName2);

        assertThrows(AuthNotFoundException.class, () -> credentials(dima.id(), credentialsName2));
    }

    @Test
    public void addToSubjectMultipleSameCredentialsConcurrent() throws InterruptedException {
        final int N = 10;
        final var readyLatch = new CountDownLatch(N);
        final var doneLatch = new CountDownLatch(N);
        final var executor = Executors.newFixedThreadPool(N);
        final var failed = new AtomicBoolean(false);

        var dima = createSubject("Dima", SubjectType.USER);
        var credentialsName = "Scotty-secure";

        for (int i = 0; i < N; ++i) {
            executor.submit(() -> {
                try {
                    readyLatch.countDown();
                    readyLatch.await();

                    addCredentials(dima.id(), credentialsName);
                } catch (Exception e) {
                    failed.set(true);
                    e.printStackTrace(System.err);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        doneLatch.await();
        executor.shutdown();

        assertFalse(failed.get());

        var credentials1 = credentials(dima.id(), credentialsName);

        assertEquals(credentialsName, credentials1.name());
        assertEquals("Value", credentials1.value());
        assertEquals(CredentialsType.PUBLIC_KEY, credentials1.type());
    }

    @Test
    public void createSubjectWithDuplicatedCredentialsIsError() {
        var credentialName1 = "work-macbook";
        var credentialName2 = "virtual-vm";
        var credentials1 = new SubjectCredentials(credentialName1, "Value", CredentialsType.PUBLIC_KEY);
        var credentials2 = new SubjectCredentials(credentialName2, "Value", CredentialsType.PUBLIC_KEY,
            Instant.now().plus(Duration.ofDays(30)));

        assertThrows(AuthInternalException.class, () ->
            createSubject("Anton", SubjectType.USER, List.of(credentials1, credentials2, credentials1)));
    }

    @Test
    public void addCredentialsToNonExistentSubjectIsError() {
        var dima = createSubject("Dima", SubjectType.USER);
        var credentialsName = "Scotty-secure";

        addCredentials(dima.id(), credentialsName);
        var credentialsOfDima = credentials(dima.id(), credentialsName);

        assertEquals(credentialsName, credentialsOfDima.name());
        assertEquals("Value", credentialsOfDima.value());
        assertEquals(CredentialsType.PUBLIC_KEY, credentialsOfDima.type());

        var nonExistentSubject = new User("some-id", AuthProvider.GITHUB, "xxx");
        assertThrows(AuthInternalException.class, () ->
            addCredentials(nonExistentSubject.id(), credentialsName));
    }

    @Test
    public void addCredentialsWithSameNameButDifferentPropertiesIsError() {
        var dima = createSubject("Dima", SubjectType.USER);

        var credentialsName1 = "Scotty-secure";
        var credentialsName2 = "New-chapter";

        addCredentials(dima.id(), credentialsName1);
        addCredentials(dima.id(), credentialsName2);

        var credentials1 = credentials(dima.id(), credentialsName1);
        var credentials2 = credentials(dima.id(), credentialsName2);

        assertEquals(credentialsName1, credentials1.name());
        assertEquals("Value", credentials1.value());
        assertEquals(CredentialsType.PUBLIC_KEY, credentials1.type());
        assertEquals(credentialsName2, credentials2.name());
        assertEquals("Value", credentials2.value());
        assertEquals(CredentialsType.PUBLIC_KEY, credentials2.type());

        var credentials2ReplicaWithOtherTypeAndTtl = new SubjectCredentials(credentialsName2, "Value",
            CredentialsType.PUBLIC_KEY, Instant.now().plus(Duration.ofDays(30)));

        assertThrows(AuthUniqueViolationException.class, () ->
            subjectService.addCredentials(dima.id(), credentials2ReplicaWithOtherTypeAndTtl));
    }

    @Test
    public void createSubjectWithCredentialsWithSameNameButDifferentPropertiesIsError() {
        var credentialName1 = "work-macbook";
        var credentialName2 = "virtual-vm";
        var credentials1 = new SubjectCredentials(credentialName1, "Value", CredentialsType.PUBLIC_KEY);
        var credentials2 = new SubjectCredentials(credentialName2, "Value", CredentialsType.PUBLIC_KEY,
            Instant.now().plus(Duration.ofDays(30)));
        var credentials2ReplicaWithOtherTtl = new SubjectCredentials(credentialName2, "Value",
            CredentialsType.PUBLIC_KEY, Instant.now().plus(Duration.ofMinutes(30)));

        assertThrows(AuthInternalException.class, () ->
            createSubject("Anton", SubjectType.USER,
                List.of(credentials1, credentials2, credentials2ReplicaWithOtherTtl)));
    }

    @Override
    protected Subject subject(String id) {
        return subjectService.subject(id);
    }

    @Override
    protected Subject createSubject(String name, SubjectType subjectType) {
        return createSubject(name, subjectType, Collections.emptyList());
    }

    @Override
    protected Subject createSubject(String name, SubjectType subjectType, List<SubjectCredentials> credentials) {
        var authProvider = subjectType == SubjectType.WORKER ? AuthProvider.INTERNAL : AuthProvider.GITHUB;
        return createSubject(name, subjectType, authProvider, credentials);
    }

    @Override
    protected Subject createSubject(String name, SubjectType subjectType, AuthProvider authProvider,
                                    List<SubjectCredentials> credentials)
    {
        var request = LSS.CreateSubjectRequest.newBuilder()
            .setAuthProvider(authProvider.toProto())
            .setProviderSubjectId(name)
            .setType(subjectType.name())
            .addAllCredentials(credentials.stream().map(ProtoConverter::from).toList())
            .build();

        return subjectService.createSubject(authProvider, name, subjectType, credentials, md5(request));
    }

    @Override
    protected void removeSubject(String subjectId) {
        subjectService.removeSubject(subjectId);
    }

    @Override
    protected SubjectCredentials credentials(String subjectId, String name) throws NoSuchElementException {
        return subjectService.credentials(subjectId, name);
    }

    @Override
    protected void addCredentials(String subjectId, String name) {
        subjectService.addCredentials(subjectId, new SubjectCredentials(name, "Value", CredentialsType.PUBLIC_KEY));
    }

    @Override
    protected void removeCredentials(String subjectId, String name) {
        subjectService.removeCredentials(subjectId, name);
    }
}
