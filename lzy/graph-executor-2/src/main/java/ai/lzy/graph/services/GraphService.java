package ai.lzy.graph.services;

import ai.lzy.graph.LGE;
import ai.lzy.longrunning.Operation;

public interface GraphService {
    void runGraph(LGE.ExecuteGraphRequest request, Operation op) throws Exception;
}
