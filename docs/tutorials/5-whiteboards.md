## Data storage with whiteboards

### Store operation results

You can also save execution results using whiteboards. 
Just declare a dataclass with `@whiteboard` decorator and pass it as a whiteboard argument to the LzyEnv.
For example:

```python
from dataclasses import dataclass
from catboost import CatBoostClassifier

from lzy.api import op, LzyRemoteEnv, Gpu
from lzy.api.whiteboard import whiteboard
import numpy as np

@dataclass
class DataSet:
    data: np.array
    labels: np.array
    
@dataclass
@whiteboard
class GraphResult:
    dataset: DataSet = None
    model: CatBoostClassifier = None


if __name__ == '__main__':
    wb = GraphResult()
    with LzyRemoteEnv(user="<Your github username>", private_key_path="~/.ssh/private.pem", whiteboard=wb):
        wb.dataset = dataset()
        wb.model = learn(wb.dataset)
        result = predict(wb.model, np.array([9, 1]))
        wb_id = wb.id()
    print(wb_id)
```

Pass whiteboard id and its type to load your results back:
```python
with LzyRemoteEnv(user="<Your github username>", private_key_path="~/.ssh/private.pem") as env:
    wb = env.get_whiteboard(wb_id, GraphResult)
    print(wb.model)
```

### Namespaces
Each whiteboard lies within a namespace which is used for security and access control. 
Users can share data with each other by providing credentials to namespaces. 
Namespace should be specified in LzyRemoteEnv:
```python
with LzyRemoteEnv(user="<Your github username>", private_key_path="~/.ssh/private.pem", 
                  namespace='data_workflow_whiteboards') as env:
    wb.model = learn(wb.dataset)
```
### Tags
Each whiteboard is associated with a set of tags which are only used for data organization and are 
not responsible for security or access control. Tags can be specified as an argument in 
`@whiteboard` decorator. For example:
```python
@dataclass
@whiteboard(tags=['tag_1', 'tag_2'])
class GraphResult:
    dataset: DataSet = None
    model: CatBoostClassifier = None
```
Or they can be applied later:
```python
with LzyRemoteEnv(user="<Your github username>", private_key_path="~/.ssh/private.pem", namespace='data_workflow_whiteboards') as env:
    env.set_whiteboard_tag(wb_id, 'tag')
```

Whiteboards can later be queried by tags. All you have to do is:
1) Provide types which your whiteboards should be built upon. 
2) For each type specify tags: only whiteboards that were created with the given tags will queried.
3) Optionally you can specify filtering criteria (e.g. creation date range)
```python
@dataclass
@whiteboard(tags=['simple_whiteboard_tag'])
class SimpleWhiteboard:
    model: CatBoostClassifier = None
    result: List[int] = None

@dataclass
@whiteboard(tags=['another_simple_whiteboard_tag'])
class AnotherSimpleWhiteboard:
    train_dataset: List[str] = None
    
with LzyRemoteEnv(user="<Your github username>", private_key_path="~/.ssh/private.pem", namespace='data_workflow_whiteboards') as env:
    # SimpleWhiteboard will be built from all whiteboards 
    # with 'simple_whiteboard_tag' in 'data_workflow_whiteboards' namespace
    # AnotherSimpleWhiteboard will be built from all whiteboards 
    # with 'another_simple_whiteboard_tag' in 'data_workflow_whiteboards' namespace
    whiteboards = env.whiteboards([SimpleWhiteboard, AnotherSimpleWhiteboard])
```
