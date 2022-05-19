package ru.yandex.cloud.ml.platform.lzy.iam.authorization;

import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;

public interface SubjectService {

    Subject createSubject(String name, String authProvider, String providerName);

    void removeSubject(Subject subject);

    void addCredentials(Subject subject, String name, String value, String type);

    void removeCredentials(Subject subject, String name, String type);
}
