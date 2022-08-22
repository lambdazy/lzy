package ai.lzy.iam.storage.impl;

import ai.lzy.iam.BaseSubjectServiceApiTest;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.storage.db.IamDataSource;
import ai.lzy.model.db.test.DatabaseCleaner;
import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import java.util.NoSuchElementException;

public class DbSubjectServiceTest extends BaseSubjectServiceApiTest {
    public static final Logger LOG = LogManager.getLogger(DbSubjectServiceTest.class);

    private ApplicationContext ctx;
    private DbSubjectService subjectService;
    private IamDataSource storage;

    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        storage = ctx.getBean(IamDataSource.class);
        subjectService = ctx.getBean(DbSubjectService.class);
    }

    @After
    public void tearDown() {
        DatabaseCleaner.cleanup(storage);
        ctx.stop();
    }

    @Override
    protected Subject subject(String id) {
        return subjectService.subject(id);
    }

    @Override
    protected void createSubject(String id, SubjectType subjectType) {
        subjectService.createSubject(id, "provider", "providerID", subjectType);
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
        subjectService.addCredentials(subject, name, "Value", "Type");
    }

    @Override
    protected void removeCredentials(Subject subject, String name) {
        subjectService.removeCredentials(subject, name);
    }
}
