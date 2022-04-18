## Release notes

### Known issues:
1. _**Parallel tasks**_ 

Running multiple graphs in parallel is currently not supported.
2. _**Bytes/Bytearray in proxy object when writing to file**_

Proxy (_another name Pearl_) objects are used for output and input of `@op`-annotated operations and [whiteboard](tutorials/5-whiteboards.md) values, which provides possibility for lazy execution.
However, when proxy object is used for proxying bytes or bytearray, it cannot be invoked with
[file.write(proxy_object)](https://docs.python.org/3/tutorial/inputoutput.html#methods-of-file-objects).

For example, the following code will fail with `TypeError: a bytes-like object is required, not 'Pearl'`:
```python
@op
def write_big_data(data) -> None:
    output_file = open("out.bin", "wb")
    output_file.write(data)
    output_file.close()
    print(sys.getsizeof(data), "bytes")


WORKFLOW_NAME = "workflow_" + str(uuid.uuid4())

if __name__ == "__main__":
    with LzyRemoteEnv().workflow(name=WORKFLOW_NAME):
        data = b'\x07\x08\x07'
        write_big_data(data)
```