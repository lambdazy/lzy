package ai.lzy.iam;

import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.util.auth.exceptions.AuthNotFoundException;
import junitparams.JUnitParamsRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(JUnitParamsRunner.class)
public abstract class BaseSubjectServiceApiTest {
    public static final Logger LOG = LogManager.getLogger(BaseSubjectServiceApiTest.class);

    @Test
    public void createAndDeleteUserTest() {
        createAndDeleteScenario(SubjectType.USER);
    }

    @Test
    public void createAndDeleteWorkerTest() {
        createAndDeleteScenario(SubjectType.WORKER);
    }

    public void createAndDeleteScenario(SubjectType subjectType) {
        var subj1 = createSubject("user1", subjectType);
        var subj2 = createSubject("user2", subjectType);

        Subject subject1 = subject(subj1.id());
        assertEquals(subj1.id(), subject1.id());
        assertEquals(subjectType, subject1.type());

        Subject subject2 = subject(subj2.id());
        assertEquals(subj2.id(), subject2.id());
        assertEquals(subjectType, subject2.type());

        removeSubject(subject1.id());
        try {
            subject(subj1.id());
            fail();
        } catch (AuthNotFoundException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }

        subject2 = subject(subj2.id());
        assertEquals(subj2.id(), subject2.id());
        assertEquals(subjectType, subject2.type());

        removeSubject(subject2.id());
        try {
            subject(subj2.id());
            fail();
        } catch (AuthNotFoundException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }
    }

    @Test
    public void createAndRemoveUserWithCredentialsTest() {
        createAndRemoveWithCredentialsScenario(SubjectType.USER);
    }

    @Test
    public void createAndRemoveWorkerWithCredentialsTest() {
        createAndRemoveWithCredentialsScenario(SubjectType.WORKER);
    }

    public void createAndRemoveWithCredentialsScenario(SubjectType subjectType) {
        var subj1 = createSubject("user1", subjectType);
        Subject subject = subject(subj1.id());
        addCredentials(subject.id(), "1");
        addCredentials(subject.id(), "2");

        SubjectCredentials credentials1 = credentials(subject.id(), "1");
        assertEquals("1", credentials1.name());
        assertEquals("Value", credentials1.value());
        assertEquals(CredentialsType.PUBLIC_KEY, credentials1.type());
        SubjectCredentials credentials2 = credentials(subject.id(), "2");
        assertEquals("2", credentials2.name());
        assertEquals("Value", credentials2.value());
        assertEquals(CredentialsType.PUBLIC_KEY, credentials2.type());

        removeCredentials(subject.id(), "1");
        try {
            credentials(subject.id(), "1");
            fail();
        } catch (NoSuchElementException e) {
            LOG.info("Valid exception {}", e.getMessage());
        } catch (AuthNotFoundException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }

        credentials2 = credentials(subject.id(), "2");
        assertEquals("2", credentials2.name());
        assertEquals("Value", credentials2.value());
        assertEquals(CredentialsType.PUBLIC_KEY, credentials2.type());

        removeCredentials(subject.id(), "2");
        try {
            credentials(subject.id(), "2");
            fail();
        } catch (NoSuchElementException e) {
            LOG.info("Valid exception {}", e.getMessage());
        } catch (AuthNotFoundException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }
        removeSubject(subject.id());
    }

    public static Collection<Object[]> restrictEqualSubjectIdsParams() {
        return Arrays.asList(
                new Object[][]{
                        {SubjectType.USER, SubjectType.USER},
                        {SubjectType.USER, SubjectType.WORKER},
                        {SubjectType.WORKER, SubjectType.USER},
                        {SubjectType.WORKER, SubjectType.WORKER}
                }
        );
    }

    protected abstract Subject subject(String id);

    protected abstract Subject createSubject(String name, SubjectType subjectType);

    protected abstract Subject createSubject(String name, SubjectType subjectType,
                                             List<SubjectCredentials> credentials);

    protected abstract Subject createSubject(String name, SubjectType subjectType, AuthProvider authProvider,
                                             List<SubjectCredentials> credentials);

    protected abstract void removeSubject(String id);

    protected abstract SubjectCredentials credentials(String subjectId, String name) throws NoSuchElementException;

    protected abstract void addCredentials(String subjectId, String name);

    protected abstract void removeCredentials(String subjectId, String name);
}
