## Data transfer

### Python variables

ʎzy serializes all arguments and return values of an operation and transfers them between local and remote machines. 
For example, `data_set` argument and the resulting `CatBoostClassifier` will be transferred without any additional user actions:

```python
@op(gpu_count=1, gpu_type=GpuType.V100.name)
def train(data_set: Bunch) -> CatBoostClassifier:
    ...
```

### Files

ʎzy provides the custom type for working with files. Files can be used in input arguments as well as in return values:

```python
from lzy.types import File

@op
def process_file(file: File) -> File:
    do_some_processing(file)
    return file

lzy = Lzy()
with lzy.workflow("file-processing"):
    input_file = File('path/to/local/file')
    result = process_file(input_file)
    with result.open("r") as f:
        ...
```

---

In the [**next**](5-environment.md) part, we will dive into the environment setup.