from catboost import CatBoostClassifier
from sklearn import datasets
from sklearn.utils import Bunch

from lzy.api.v1 import op, Lzy, GpuType


@op
def dataset() -> Bunch:
    data_set = datasets.load_breast_cancer()
    return data_set


@op(gpu_count=1, gpu_type=GpuType.V100.name)
def train(data_set: Bunch) -> CatBoostClassifier:
    cb_model = CatBoostClassifier(
        iterations=1000, task_type="GPU", devices="0:1", train_dir="/tmp/catboost"
    )
    cb_model.fit(data_set.data, data_set.target, verbose=True)
    return cb_model


lzy = Lzy()
with lzy.workflow("training"):
    data = dataset()
    model = train(data)
    result = model.predict(data.data[0])
    print(result)
