package ru.yandex.cloud.ml.platform.model.util.integer;

public interface SharedIntegerManager {

    SharedIntegerManager withPrefix(String prefix);

    SharedInteger get(String key, int defaultValue);
}
