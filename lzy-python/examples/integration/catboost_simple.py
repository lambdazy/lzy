from dataclasses import dataclass
from catboost import CatBoostClassifier

from api.env import RunConfig
from lzy.api import op, LzyRemoteEnv, Gpu
import numpy as np

from lzy.servant.terminal_server import TerminalConfig


@dataclass
class DataSet:
    data: np.array
    labels: np.array


@op
def dataset() -> DataSet:
    train_data = np.array([[0, 3],
                           [4, 1],
                           [8, 1],
                           [9, 1]])
    train_labels = np.array([0, 0, 1, 1])
    return DataSet(train_data, train_labels)


@op(gpu=Gpu.any())
def learn(data_set: DataSet) -> CatBoostClassifier:
    cb_model = CatBoostClassifier(iterations=1000, task_type="CPU", devices='0:1', train_dir='/tmp/catboost')
    cb_model.fit(data_set.data, data_set.labels, verbose=False)
    return cb_model


@op
def predict(cb_model: CatBoostClassifier, point: np.array) -> np.int64:
    return cb_model.predict(point)


if __name__ == '__main__':
    with LzyRemoteEnv():
        data = dataset()
        model = learn(data)
        result = predict(model, np.array([9, 1]))
    print(result)
