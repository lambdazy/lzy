## Setup environment

### Default behavior

ʎzy automatically captures the python version with all imported modules and generates a conda environment specification.
All local modules are serialized using [cloudpickle](https://github.com/cloudpipe/cloudpickle).
Before the user function runs on a remote VM, ʎzy applies the prepared conda environment and loads all serialized local modules.
Therefore, **there is no need for manual environment setup in a general case**.

### Manual customization

For more complex scenarios (e.g., if dependencies beyond python are required), you can manually set up conda environment using its [yaml representation](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-file-manually):

```python
with LzyRemoteEnv(conda_yaml_path='path/to/custom/conda.yml'):
    ...
```

