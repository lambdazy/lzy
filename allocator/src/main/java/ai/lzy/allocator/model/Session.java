package ai.lzy.allocator.model;

import java.time.Duration;

public record Session(String sessionId, String owner, Duration minIdleTimeout) {}
