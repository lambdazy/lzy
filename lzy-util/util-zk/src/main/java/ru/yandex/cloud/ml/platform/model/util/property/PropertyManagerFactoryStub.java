package ru.yandex.cloud.ml.platform.model.util.property;

import org.springframework.stereotype.Service;

@Service("PropertyManagerFactoryStub")
public class PropertyManagerFactoryStub implements PropertyManagerFactory {
    @Override
    public <T> PropertyManager<T> propertyManager(T initialValue,
                                                  Class<T> tClass,
                                                  String configKey,
                                                  boolean updateTagsOnStartup) {
        return new PropertyManagerStub<>(initialValue);
    }
}
