# noinspection PyPackageRequirements
import numpy as np
# noinspection PyPackageRequirements
from catboost import CatBoostClassifier

from lzy.api.v1 import CpuType
from lzy.injections.catboost import inject_catboost

inject_catboost()

if __name__ == "__main__":
    data = np.array([[0, 3], [4, 1], [8, 1], [9, 1]])
    labels = np.array([0, 0, 1, 1])

    model = CatBoostClassifier(iterations=1000, train_dir="/tmp/catboost")
    # noinspection PyArgumentList
    model.fit(data, labels, cpu_type=str(CpuType.CASCADE_LAKE.value))

    result = model.predict(np.array([9, 1]))
    print("Prediction: " + str(result))
