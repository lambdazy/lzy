## Managing graph results

### Views

Whiteboards may contain more information than is needed for business logic.
For this case, ʎzy provides a `Views` mechanism that allows automatically building more restricted objects.

For instance, if we need only a trained model without metrics and parameters, we can define a method that provides such view:

```python
@dataclass
@whiteboard(tags=['best_model'])
class BestModel:
    model: Optional[CatBoostClassifier] = None
    params: Optional[Dict[str, int]] = None
    score: float = 0.0
    
    @view
    def model_view(self) -> CatBoostClassifier:
        return self.model
```

And then automatically load only view instances:

```python
models = env.whiteboards([BestModel]).views(CatBoostClassifier)
```