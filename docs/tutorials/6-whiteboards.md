## Managing graph results

### Whiteboards

Whiteboard is a way how graph execution results can be stored and versioned in ʎzy.
Whiteboard can be declared in a form of a `dataclass`: 

```python
@dataclass
@whiteboard(tags=['best_model'])
class BestModel:
    model: Optional[CatBoostClassifier] = None
    params: Optional[Dict[str, int]] = None
    score: float = 0.0
```

Note that it is necessary to specify tags for a Whiteboard. Tags are used for querying.

Whiteboard can be filled in during the graph execution:

```python
@op
def dataset() -> Bunch:
    data_set = datasets.load_breast_cancer()
    return data_set


@op(gpu=Gpu.any())
def search_best_model(data_set: Bunch) -> GridSearchCV:
    grid = {'max_depth': [3, 4, 5], 'n_estimators': [100, 200, 300]}
    cb_model = CatBoostClassifier()
    search = GridSearchCV(estimator=cb_model, param_grid=grid, scoring='accuracy', cv=5)
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
```

Whiteboard can be loaded by `id`:

```python
wb = env.whiteboard('<id>', BestModel)
print(wb.params['max_depth'])
```

Another way is to load all whiteboards for the given type and creation datetime:
```python
wbs = env.whiteboards([BestModel], from_date=..., to_date=...)
```

---

In the [**next**](7-views.md) part, we will show how to load only data that is really needed for the business task.

