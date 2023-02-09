## Setup environment

### Default behavior

ʎzy automatically captures the python version with all imported modules and generates a conda environment specification.
All local modules are uploaded to the internal storage. Before an @op-function runs on a remote VM, ʎzy applies the
prepared conda environment and downloads all local modules. Therefore, **there is no need for manual environment setup
in a general case**.

### Manual customization

#### Conda environment

For more complex scenarios (e.g., if dependencies beyond python are required), you can manually set up a conda
environment using
its [YAML representation](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-file-manually):

```python
lzy = Lzy()
with lzy.workflow("train", conda_yaml_path='path/to/custom/conda.yml'):
    ...
```

#### Local modules

You also can manually define which local modules should be uploaded:

```python
lzy = Lzy()
with lzy.workflow("train", local_modules_path=['/path/to/local/module']):
    ...
```

#### Python libraries

It is possible to override versions of installed locally python libraries or add additional libraries.
For example, if you want to run @op with catboost version "1.1.0", you can set it for a workflow:

```python
lzy = Lzy()
with lzy.workflow("train", libraries={"catboost": "1.1.0"}):
    ...
```

#### Docker image

If an environment requires deep specification, it is possible to define docker image for a workflow:

```python
lzy = Lzy()
with lzy.workflow("train", docker_image='<tag>'):
    ...
```

Please note that a docker image should contain conda or be built as follows:

```dockerfile
FROM lzydock/worker-base:master-1.1
...
```

#### Op specification

All previous settings can be applied to @op only:

```python
@op(docker_image='<tag>', local_modules_path=['/path/to/local/module'], libraries={"catboost": "1.1.0"})
def train(data_set: Bunch) -> CatBoostClassifier:
    ...
```

---

In the [**next**](6-whiteboards.md) part, we will detail how graph results can be stored in ʎzy.
