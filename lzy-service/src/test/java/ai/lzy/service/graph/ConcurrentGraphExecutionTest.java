package ai.lzy.service.graph;

import org.junit.Test;

import static ai.lzy.longrunning.test.IdempotencyUtilsTest.processConcurrently;

public class ConcurrentGraphExecutionTest extends AbstractGraphExecutionTest {

    @Test
    public void executeSimpleGraph() throws InterruptedException {
        processConcurrently(executeSimpleGraphScenario());
    }

    @Test
    public void failedWithEmptyGraph() throws InterruptedException {
        processConcurrently(emptyGraphScenario());
    }

    @Test
    public void failedWithDuplicatedOutputSlotUris() throws InterruptedException {
        processConcurrently(duplicatedOutputSlotUriScenario());
    }

    @Test
    public void failedWithCyclicDataflowGraph() throws InterruptedException {
        processConcurrently(cyclicDataflowGraphScenario());
    }

    @Test
    public void failedWithUnknownInputSlotUri() throws InterruptedException {
        processConcurrently(unknownInputSlotUriScenario());
    }

    @Test
    public void failedWithoutSuitableZone() throws InterruptedException {
        processConcurrently(withoutSuitableZoneScenario());
    }

    @Test
    public void failedWithNonSuitableZone() throws InterruptedException {
        processConcurrently(nonSuitableZoneScenario());
    }
}
