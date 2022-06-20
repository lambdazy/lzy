package ru.yandex.cloud.ml.platform.lzy.graph.exec;

import ru.yandex.cloud.ml.platform.lzy.graph.algo.GraphBuilder.ChannelEdge;

public interface ChannelChecker {
    boolean ready(ChannelEdge edge);
}
