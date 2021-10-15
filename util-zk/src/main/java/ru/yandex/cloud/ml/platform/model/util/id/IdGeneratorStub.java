package ru.yandex.cloud.ml.platform.model.util.id;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Lazy
@Component("IdGeneratorStub")
public class IdGeneratorStub implements IdGenerator {
    @Override
    public String generate() {
        return UUID.randomUUID().toString();
    }
}
