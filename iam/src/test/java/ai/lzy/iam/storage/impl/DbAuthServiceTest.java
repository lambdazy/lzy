package ai.lzy.iam.storage.impl;

import ai.lzy.iam.BaseAuthServiceApiTest;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.storage.db.IamDataSource;
import ai.lzy.model.db.test.DatabaseCleaner;
import ai.lzy.util.auth.credentials.Credentials;
import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;

public class DbAuthServiceTest extends BaseAuthServiceApiTest {
    public static final Logger LOG = LogManager.getLogger(DbAuthServiceTest.class);

    private ApplicationContext ctx;
    private DbSubjectService subjectService;
    private IamDataSource storage;
    private DbAuthService authenticateService;

    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        storage = ctx.getBean(IamDataSource.class);
        subjectService = ctx.getBean(DbSubjectService.class);
        authenticateService = ctx.getBean(DbAuthService.class);
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
    protected void createSubject(String id, String name, String value, SubjectType subjectType) {
        subjectService.createSubject(id, "", "", subjectType);
    }

    @Override
    protected void removeSubject(Subject subject) {
        subjectService.removeSubject(subject);
    }

    @Override
    protected void addCredentials(Subject subject, String name, String value, String type) {
        subjectService.addCredentials(subject, name, value, type);
    }

    @Override
    protected void removeCredentials(Subject subject, String name) {
        subjectService.removeCredentials(subject, name);
    }

    @Override
    protected void authenticate(Credentials credentials) {
        authenticateService.authenticate(credentials);
    }
}
