from catboost import CatBoostClassifier
from sklearn import datasets
from sklearn.utils import Bunch

from lzy.api.v1 import Gpu, LzyRemoteEnv, op


@op
def dataset() -> Bunch:
    data_set = datasets.load_breast_cancer()
    return data_set


@op(gpu=Gpu.any())
def train(data_set: Bunch) -> CatBoostClassifier:
    cb_model = CatBoostClassifier(
        iterations=1000, task_type="GPU", devices="0:1", train_dir="/tmp/catboost"
    )
    cb_model.fit(data_set.data, data_set.target, verbose=True)
    return cb_model


env = LzyRemoteEnv()
with env.workflow("training"):
    data_set = dataset()
    model = train(data_set)
    result = model.predict(data_set.data[0])
    print(result)
