import sys
import cloudpickle


def main():
    args = sys.argv[1:]
    in_file_path = args[0]
    out_file_path = args[1]

    print('RUN startup.py')
    print(f'reading from file {in_file_path}')
    print('write something to stderr', file=sys.stderr)

    with open(in_file_path, 'rb') as in_handle:
        lzy_op = cloudpickle.load(in_handle)
        lzy_op.deploy()
        result = lzy_op.materialize()

        print(f'result of execution {result}')
        print(f'writing to file {out_file_path}')
        with open(out_file_path, 'wb') as out_handle:
            cloudpickle.dump(result, out_handle)


if __name__ == "__main__":
    main()
