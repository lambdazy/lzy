package ru.yandex.cloud.ml.platform.model.util.property;

public interface PropertyManagerFactory {

    <T> PropertyManager<T> propertyManager(T initialValue,
        Class<T> tClass,
        String configKey,
        boolean updateTagsOnStartup);
}
