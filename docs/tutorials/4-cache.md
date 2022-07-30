## Data caching

An unexpected exception can be raised during graph execution, e.g.:

```python
env = LzyRemoteEnv(namespace='prod')
with env.workflow(name='MyExperiment'):
    data_set = dataset()
    model = train(data_set)                                     # exception is raised here
    result = model.predict(data_set.data[0])
    print(result)
```

ʎzy allows re-running graph without re-running all the operations.
In this case, the results of all completed operations will be loaded from the cache.
To enforce such behavior set `cache_policy` to `SAVE_AND_RESTORE`:

```python
env = LzyRemoteEnv()
with env.workflow("training", cache_policy=CachePolicy.SAVE_AND_RESTORE):
    data_set = dataset()                                        # data_set is loaded from cache
    model = train(data_set)
    result = model.predict(data_set.data[0])
    print(result)
```

---

In the [**next**](5-whiteboards.md) part, we will detail how graph results can be stored in ʎzy.
