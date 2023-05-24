package ai.lzy.iam.resources.subjects;

import com.google.protobuf.Any;
import com.google.protobuf.TextFormat;

public record External(
    String id,
    Any details
) implements Subject {

    @Override
    public SubjectType type() {
        return SubjectType.EXTERNAL;
    }

    @Override
    public String str() {
        return "External(" + id() + ", " + TextFormat.printer().shortDebugString(details) + ')';
    }
}
