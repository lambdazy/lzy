from dataclasses import dataclass

import jsonpickle
from pure_protobuf.dataclasses_ import message


@message
@dataclass
class Wb:
    a: int
    b: int


if __name__ == '__main__':
    a = Wb()
