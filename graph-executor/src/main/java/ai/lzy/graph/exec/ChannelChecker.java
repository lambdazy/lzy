package ai.lzy.graph.exec;

import ai.lzy.graph.algo.GraphBuilder.ChannelEdge;

public interface ChannelChecker {
    boolean ready(ChannelEdge edge);
}
