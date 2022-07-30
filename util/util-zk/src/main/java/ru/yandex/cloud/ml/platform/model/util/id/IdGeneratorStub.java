package ru.yandex.cloud.ml.platform.model.util.id;

import java.util.UUID;

public class IdGeneratorStub implements IdGenerator {

    @Override
    public String generate() {
        return UUID.randomUUID().toString();
    }
}
