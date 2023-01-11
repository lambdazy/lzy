from dataclasses import dataclass
from typing import Dict, Optional

from catboost import CatBoostClassifier
from sklearn import datasets
from sklearn.model_selection import GridSearchCV
from sklearn.utils import Bunch

from lzy.api.v1 import Gpu, LzyRemoteEnv, op, view, whiteboard


@dataclass
@whiteboard(tags=["best_model"])
class BestModel:
    model: Optional[CatBoostClassifier] = None
    params: Optional[Dict[str, int]] = None
    score: float = 0.0

    @view
    def model_view(self) -> CatBoostClassifier:
        return self.model


@op
def dataset() -> Bunch:
    data_set = datasets.load_breast_cancer()
    return data_set


@op(gpu=Gpu.any())
def search_best_model(data_set: Bunch) -> GridSearchCV:
    grid = {"max_depth": [3, 4, 5], "n_estimators": [100, 200, 300]}
    cb_model = CatBoostClassifier()
    search = GridSearchCV(estimator=cb_model, param_grid=grid, scoring="accuracy", cv=5)
    search.fit(data_set.data, data_set.target)
    return search


env = LzyRemoteEnv()
wb = BestModel()
with env.workflow("training", whiteboard=wb):
    data_set = dataset()
    search = search_best_model(data_set)
    wb.model = search.best_estimator_
    wb.params = search.best_params_
    wb.score = search.best_score_
    print(wb.__id__)

# loaded_wb = env.whiteboard(wb.__id__, BestModel)
loaded_wb = env.whiteboard_by_id(wb.__id__)
print(loaded_wb.params["max_depth"])

wbs = env.whiteboards([BestModel])
print(wbs)

views = env.whiteboards([BestModel]).views(CatBoostClassifier)
print(views)
