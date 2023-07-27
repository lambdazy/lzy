package ai.lzy.scheduler.test;

import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.scheduler.test.mocks.AllocatedWorkerMock;
import ai.lzy.scheduler.test.mocks.AllocatorMock;
import ai.lzy.v1.common.LME;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.scheduler.Scheduler;
import ai.lzy.v1.scheduler.SchedulerApi.TaskScheduleRequest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class IntegrationTest extends IamOnlySchedulerContextTests {
    @Test
    public void testSimple() throws Exception {
        var allocate = new CountDownLatch(1);
        var latch = new CountDownLatch(1);
        var exec = new CountDownLatch(1);

        final var port = FreePortFinder.find(1000, 2000);

        final var worker = new AllocatedWorkerMock(port, (a) -> {
            exec.countDown();
            return true;
        });

        AllocatorMock.onAllocate = (a, b, c) -> {
            allocate.countDown();
            return "localhost:" + port;
        };

        AllocatorMock.onDestroy = (a) -> latch.countDown();

        var resp = stub.schedule(TaskScheduleRequest.newBuilder()
            .setWorkflowId("wfid")
            .setWorkflowName("wf")
            .setUserId("uid")
            .setTask(LMO.TaskDesc.newBuilder()
                .setOperation(LMO.Operation.newBuilder()
                    .setName("name")
                    .setRequirements(LMO.Requirements.newBuilder()
                        .setPoolLabel("s")
                        .setZone("a").build())
                    .setCommand("")
                    .setDescription("")
                    .setEnv(LME.EnvSpec.newBuilder().build())
                    .build())
                .build())
            .build());

        allocate.await();
        exec.await();
        latch.await();

        var status = awaitCompleted(resp.getStatus().getTaskId(), resp.getStatus().getWorkflowId());

        Assert.assertTrue(status.hasSuccess());
    }

    @Test
    public void testParallel() throws Exception {
        var allocate = new CountDownLatch(2);
        var latch = new CountDownLatch(2);
        var exec = new CountDownLatch(2);

        final var port1 = FreePortFinder.find(1000, 2000);

        final var worker1 = new AllocatedWorkerMock(port1, (a) -> {
            exec.countDown();
            return true;
        });

        final var port2 = FreePortFinder.find(1000, 2000);

        final var worker2 = new AllocatedWorkerMock(port2, (a) -> {
            exec.countDown();
            return true;
        });

        AllocatorMock.onAllocate = (a, b, c) -> {
            allocate.countDown();

            AllocatorMock.onAllocate = (a1, b1, c1) -> {
                allocate.countDown();

                return "localhost:" + port2;
            };
            return "localhost:" + port1;
        };

        AllocatorMock.onDestroy = (a) -> latch.countDown();

        var resp1 = stub.schedule(TaskScheduleRequest.newBuilder()
            .setWorkflowId("wfid")
            .setWorkflowName("wf")
            .setUserId("uid")
            .setTask(LMO.TaskDesc.newBuilder()
                .setOperation(LMO.Operation.newBuilder()
                    .setName("name")
                    .setRequirements(LMO.Requirements.newBuilder()
                        .setPoolLabel("s")
                        .setZone("a").build())
                    .setCommand("")
                    .setDescription("")
                    .setEnv(LME.EnvSpec.newBuilder().build())
                    .build())
                .build())
            .build());

        var resp2 = stub.schedule(TaskScheduleRequest.newBuilder()
            .setWorkflowId("wfid")
            .setWorkflowName("wf")
            .setUserId("uid")
            .setTask(LMO.TaskDesc.newBuilder()
                .setOperation(LMO.Operation.newBuilder()
                    .setName("name")
                    .setRequirements(LMO.Requirements.newBuilder()
                        .setPoolLabel("s")
                        .setZone("a").build())
                    .setCommand("")
                    .setDescription("")
                    .setEnv(LME.EnvSpec.newBuilder().build())
                    .build())
                .build())
            .build());

        allocate.await();
        exec.await();
        latch.await();

        var status1 = awaitCompleted(resp1.getStatus().getTaskId(), resp1.getStatus().getWorkflowId());
        var status2 = awaitCompleted(resp2.getStatus().getTaskId(), resp2.getStatus().getWorkflowId());

        Assert.assertTrue(status1.hasSuccess());
        Assert.assertTrue(status2.hasSuccess());
    }

    @Test
    public void testFailExec() throws Exception {
        var allocate = new CountDownLatch(1);
        var latch = new CountDownLatch(1);
        var exec = new CountDownLatch(1);

        final var port = FreePortFinder.find(1000, 2000);

        final var worker = new AllocatedWorkerMock(port, (a) -> {
            exec.countDown();
            return false;
        });

        AllocatorMock.onAllocate = (a, b, c) -> {
            allocate.countDown();
            return "localhost:" + port;
        };

        AllocatorMock.onDestroy = (a) -> latch.countDown();

        var resp = stub.schedule(TaskScheduleRequest.newBuilder()
            .setWorkflowId("wfid")
            .setWorkflowName("wf")
            .setUserId("uid")
            .setTask(LMO.TaskDesc.newBuilder()
                .setOperation(LMO.Operation.newBuilder()
                    .setName("name")
                    .setRequirements(LMO.Requirements.newBuilder()
                        .setPoolLabel("s")
                        .setZone("a").build())
                    .setCommand("")
                    .setDescription("")
                    .setEnv(LME.EnvSpec.newBuilder().build())
                    .build())
                .build())
            .build());

        allocate.await();
        exec.await();
        latch.await();

        var status = awaitCompleted(resp.getStatus().getTaskId(), resp.getStatus().getWorkflowId());

        Assert.assertTrue(status.hasError());
    }

    @Test
    public void testFailAllocate() throws Exception {
        var allocate = new CountDownLatch(1);

        AllocatorMock.onAllocate = (a, b, c) -> {
            allocate.countDown();
            throw new RuntimeException("");
        };

        var resp = stub.schedule(TaskScheduleRequest.newBuilder()
            .setWorkflowId("wfid")
            .setWorkflowName("wf")
            .setUserId("uid")
            .setTask(LMO.TaskDesc.newBuilder()
                .setOperation(LMO.Operation.newBuilder()
                    .setName("name")
                    .setRequirements(LMO.Requirements.newBuilder()
                        .setPoolLabel("s")
                        .setZone("a").build())
                    .setCommand("")
                    .setDescription("")
                    .setEnv(LME.EnvSpec.newBuilder().build())
                    .build())
                .build())
            .build());

        allocate.await();

        var status = awaitCompleted(resp.getStatus().getTaskId(), resp.getStatus().getWorkflowId());
        Assert.assertTrue(status.hasError());
    }

    private Scheduler.TaskStatus awaitCompleted(String taskId, String workflowId) throws InterruptedException {
        var retries = 0;
        Scheduler.TaskStatus taskStatus = null;
        while ((++retries) < 10) {   // Waiting for task fail
            var r = stub.status(ai.lzy.v1.scheduler.SchedulerApi.TaskStatusRequest.newBuilder()
                .setTaskId(taskId)
                .setWorkflowId(workflowId)
                .build());

            if (!r.getStatus().hasExecuting()) {
                taskStatus = r.getStatus();
                break;
            }
            Thread.sleep(100);
        }

        if (taskStatus == null) {
            Assert.fail();  // Retries exceeded
        }

        return taskStatus;
    }
}
