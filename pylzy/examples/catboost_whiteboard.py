from dataclasses import dataclass
from typing import Optional

from catboost import CatBoostClassifier
from sklearn import datasets
from sklearn.model_selection import GridSearchCV
from sklearn.utils import Bunch

from lzy.api.v1 import Lzy, op, whiteboard, GpuType


@dataclass
@whiteboard(name="best_model")
class BestModel:
    model: Optional[CatBoostClassifier] = None
    score: float = 0.0


@op
def dataset() -> Bunch:
    data_set = datasets.load_breast_cancer()
    return data_set


@op(gpu_count=1, gpu_type=GpuType.V100.name)
def search_best_model(data_set: Bunch) -> GridSearchCV:
    grid = {"max_depth": [3, 4, 5], "n_estimators": [100, 200, 300]}
    cb_model = CatBoostClassifier()
    search = GridSearchCV(estimator=cb_model, param_grid=grid, scoring="accuracy", cv=5)
    search.fit(data_set.data, data_set.target)
    return search


lzy = Lzy()
with lzy.workflow("training") as wf:
    wb = wf.create_whiteboard(BestModel, tags=["training", "catboost"])
    data = dataset()
    search = search_best_model(data)
    wb.model = search.best_estimator_
    wb.score = float(search.best_score_)
    print(wb.id)

loaded_wb = lzy.whiteboard(id_=wb.id)
print(loaded_wb.score)

wbs = list(lzy.whiteboards(name="best_model"))
print(len(wbs))
