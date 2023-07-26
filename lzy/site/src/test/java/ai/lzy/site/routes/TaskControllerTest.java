package ai.lzy.site.routes;

import ai.lzy.site.routes.context.IamAndSchedulerSiteContextTests;
import io.micronaut.http.HttpStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static ai.lzy.site.routes.SchedulerTasksMock.getTestTasks;

public class TaskControllerTest extends IamAndSchedulerSiteContextTests {
    private Tasks tasks;

    @Before
    public void before() {
        tasks = micronautContext().getBean(Tasks.class);
    }

    @Test
    public void getTasksTest() {
        final String signInUrl = "https://host/signIn";
        final var response = auth.acceptGithubCode("code", signInUrl);
        Assert.assertEquals(HttpStatus.MOVED_PERMANENTLY.getCode(), response.code());
        final Utils.ParsedCookies cookies = Utils.parseCookiesFromHeaders(response);
        final String workflowId = "workflowId";
        {
            final var tasksListResponse = tasks.get(
                cookies.userSubjectId(),
                cookies.sessionId(),
                new Tasks.GetTasksRequest(workflowId)
            );
            final Tasks.GetTasksResponse body = tasksListResponse.body();
            Assert.assertNotNull(body);
            final List<Tasks.TaskStatus> taskStatusList = body.taskStatusList();
            Assert.assertNotNull(taskStatusList);

            Assert.assertEquals(getTestTasks(workflowId), taskStatusList);
        }
    }
}
