package ai.lzy.iam.resources.subjects;

import yandex.cloud.auth.api.Subject;

public record YcSubject(
    yandex.cloud.auth.api.Subject subject
) implements ExternalSubject<yandex.cloud.auth.api.Subject> {

    @Override
    public String id() {
        if (subject instanceof Subject.NamedId named) {
            return named.getId();
        }
        return subject.toId().toString();
    }

    @Override
    public SubjectType type() {
        return SubjectType.EXTERNAL_YC;
    }

    @Override
    public Subject provided() {
        return subject;
    }
}
