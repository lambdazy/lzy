package ai.lzy.test.context;

import ai.lzy.graph.test.GraphExecutorDecorator;
import io.micronaut.context.ApplicationContext;

public interface GraphExecutorBeans {
    ApplicationContext graphExecutorContext();

    default GraphExecutorDecorator graphExecutorDecorator() {
        return graphExecutorContext().getBean(GraphExecutorDecorator.class);
    }
}
