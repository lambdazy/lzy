from catboost import CatBoostClassifier
from sklearn import datasets
from sklearn.utils import Bunch

from lzy.api.v1 import op, Lzy, GpuType


@op
def dataset() -> Bunch:
    data_set = datasets.load_breast_cancer()
    return data_set


@op(gpu_count=1, gpu_type=str(GpuType.V100.value))
def train(data_set: Bunch) -> CatBoostClassifier:
    cb_model = CatBoostClassifier(
        iterations=1000, task_type="GPU", devices="0:1", train_dir="/tmp/catboost"
    )
    cb_model.fit(data_set.data, data_set.target, verbose=True)
    return cb_model


lzy = Lzy()
lzy.auth(user="mrMakaronka", key_path="/Users/tomato/Work/keys/private.pem", endpoint="158.160.44.118:8122",
         whiteboards_endpoint="158.160.34.24:8122")
with lzy.workflow("training"):
    data = dataset()
    model = train(data)
    result = model.predict(data.data[0])
    print(result)
