from argparse import ArgumentParser
import inspect
import random


def generate():
    return {
        'gen_0': random.randint(0, 100),
        'gen_1': random.randint(0, 100)
    }


def increment(x: int) -> int:
    return x + 1


def multiply(x: int) -> int:
    return x * 2


def add(x: int, y: int) -> int:
    return x + y


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-i', '--input', type=list)
    parser.add_argument('-o', '--output', type=list)
    parser.add_argument('-c', '--command', choices=['gen', 'inc', 'mul', 'add'], required=True)

    args = parser.parse_args()

    runnable = {
        'gen': generate,
        'inc': increment,
        'mul': multiply,
        'add': add
    }[args.command]

    input_names = inspect.getfullargspec(runnable).args
    print(input_names)
    inputs = {}
    for input_name, input_file in zip(input_names, args.input):
        with open(input_file, 'r') as fd:
            inputs[input_name] = fd.read()

    result = runnable(**inputs)

    if not isinstance(result, dict):
        result = {args.command: result}

    for output_description in args.output:
        output_name, output_file = output_description.split('=')
        with open(output_file, 'w') as fd:
            fd.write(str(result[output_name]))
