# noinspection PyPackageRequirements
import numpy as np
# noinspection PyPackageRequirements
from catboost import CatBoostClassifier, Pool

from lzy.v1.api import Provisioning

if __name__ == '__main__':
    train_data = Pool(data=[[1, 4, 5, 6],
                            [4, 5, 6, 7],
                            [30, 40, 50, 60]],
                      label=[1, 1, -1],
                      weight=[0.1, 0.2, 0.3])

    model = CatBoostClassifier(iterations=1000, train_dir='/tmp/catboost')
    # noinspection PyArgumentList
    model.fit(train_data, provisioning=Provisioning())

    result = model.predict(np.array([1, 4, 5, 6]))
    print("Prediction: " + str(result))
