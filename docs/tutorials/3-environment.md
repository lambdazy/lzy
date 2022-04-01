## Setup environment

### Default behavior

ʎzy automatically captures the python version with all imported modules and generates a conda environment specification.
All local modules are uploaded to the internal storage. Before an @op-function runs on a remote VM, ʎzy applies the
prepared conda environment and downloads all local modules. Therefore, **there is no need for manual environment setup
in a general case**.

### Manual customization

For more complex scenarios (e.g., if dependencies beyond python are required), you can manually set up a conda
environment using
its [YAML representation](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-file-manually):

```python
env = LzyRemoteEnv()
with env.workflow("train", conda_yaml_path='path/to/custom/conda.yml'):
    ...
```

You also can manually define which local modules should be uploaded:

```python
env = LzyRemoteEnv()
with env.workflow("train", local_module_paths=['/path/to/local/module']):
    ...
```

---

In the [**next**](4-cache.md) part, we will touch the data caching policies.
