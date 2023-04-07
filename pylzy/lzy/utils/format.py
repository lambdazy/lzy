import inspect


def pretty_function(func):
    name = func.__name__
    typename = type(func).__name__
    filename = None
    line_no = None

    if inspect.ismethod(func):
        func = func.__func__

    if inspect.isfunction(func):
        filename = inspect.getfile(func)
        if hasattr(func, '__code__'):
            line_no = func.__code__.co_firstlineno

    result = f'{typename} `{name}`'
    if filename:
        result = f'{result} at {filename}'

        if line_no is not None:
            result = f'{result}:{line_no}'

    return result
