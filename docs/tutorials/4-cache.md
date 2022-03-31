## Restarting after failures

### Restart policy

For each workflow execution name is specified.
In case there is a failure during execution workflow can be restarted with the following restart policies:
1. `FROM_LAST_UNCOMPLETED_SNAPSHOT`:
   Execution results for last uncompleted snapshot with the same workflow name will be used to resume 
calculations from the breakpoint.
2. `FROM_LAST_SNAPSHOT`:
   Execution results for last snapshot (completed or uncompleted) with the same workflow name will be used to resume
   calculations from the breakpoint.

```python
env = LzyRemoteEnv(namespace='prod')
with env.workflow(name='MyExperiment', restart_policy=FROM_LAST_SNAPSHOT):
    data = dataset()
    model = learn(data)
    result = model.predict(np.array([9, 1]))
```