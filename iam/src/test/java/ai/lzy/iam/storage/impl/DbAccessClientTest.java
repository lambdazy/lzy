package ai.lzy.iam.storage.impl;

import ai.lzy.iam.BaseAccessServiceApiTest;
import ai.lzy.iam.resources.*;
import ai.lzy.iam.resources.impl.Whiteboard;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.storage.db.IamDataSource;
import ai.lzy.model.db.test.DatabaseCleaner;
import ai.lzy.util.auth.exceptions.AuthBadRequestException;
import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class DbAccessClientTest extends BaseAccessServiceApiTest {
    public static final Logger LOG = LogManager.getLogger(DbAccessClientTest.class);

    private ApplicationContext ctx;
    private DbSubjectService subjectService;
    private IamDataSource storage;
    private DbAccessClient accessClient;
    private DbAccessBindingClient accessBindingClient;

    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        storage = ctx.getBean(IamDataSource.class);
        subjectService = ctx.getBean(DbSubjectService.class);
        accessClient = ctx.getBean(DbAccessClient.class);
        accessBindingClient = ctx.getBean(DbAccessBindingClient.class);
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
    protected void setAccessBindings(AuthResource resource, List<AccessBinding> accessBinding) {
        accessBindingClient.setAccessBindings(resource, accessBinding);
    }

    @Override
    protected Stream<AccessBinding> listAccessBindings(AuthResource resource) {
        return accessBindingClient.listAccessBindings(resource);
    }

    @Override
    protected void updateAccessBindings(AuthResource resource, List<AccessBindingDelta> accessBinding) {
        accessBindingClient.updateAccessBindings(resource, accessBinding);
    }

    @Override
    protected boolean hasResourcePermission(Subject subject, AuthResource resource, AuthPermission permission) {
        return accessClient.hasResourcePermission(subject, resource.resourceId(), permission);
    }

    @After
    public void tearDown() {
        DatabaseCleaner.cleanup(storage);
        ctx.stop();
    }
}
