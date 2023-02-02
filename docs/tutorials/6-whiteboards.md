## Managing graph results

### Whiteboards

Whiteboard is a way how graph execution results can be stored and versioned in ʎzy.
Whiteboard can be declared in a form of a `dataclass`:

```python
@dataclass
@whiteboard(name="best_model")
class BestModel:
    model: Optional[CatBoostClassifier] = None
    score: float = 0.0
```

Note that it is necessary to specify name for a Whiteboard. Names are used for querying.

Whiteboard can be filled in during the graph execution:

```python
@op
def dataset() -> Bunch:
    data_set = datasets.load_breast_cancer()
    return data_set


@op(gpu_count=1, gpu_type=GpuType.V100.name)
def search_best_model(data_set: Bunch) -> GridSearchCV:
    grid = {'max_depth': [3, 4, 5], 'n_estimators': [100, 200, 300]}
    cb_model = CatBoostClassifier()
    search = GridSearchCV(estimator=cb_model, param_grid=grid, scoring='accuracy', cv=5)
    search.fit(data_set.data, data_set.target)
    return search


lzy = Lzy()
with lzy.workflow("training") as wf:
    wb = wf.create_whiteboard(BestModel, tags=["training", "catboost"])
    data_set = dataset()
    search = search_best_model(data_set)
    wb.model = search.best_estimator_
    wb.score = search.best_score_
    print(wb.id)
```

Whiteboard can be loaded by `id`:

```python
wb = lzy.whiteboard('<id>')
print(wb.score)
```

Another way is to load all whiteboards for the given name, tags, and creation datetime:

```python
wbs = lzy.whiteboards(name="best_model", tags=["training"], not_before=..., not_after=...)
```

---

In the [**next**](7-integrations.md) part, we will demonstrate how ʎzy is integrated with popular open-source libraries.

