import abc
from enum import Enum
from typing import List


class ObjectDAO:
    id: str
    data: dict  # сюда лягут все атрибуты объекта предметной области
    name: str  # имя объекта
    data_page_id: str


class RelationType(Enum):
    STRONG = 'strong'
    WEAK = 'weak'


class Relation:
    source_obj_id: str
    target_obj_id: str
    data_page_id: str
    type: RelationType


class DataPageSource(Enum):
    NIRVANA = 'nirvana'


# Тут я всего не знаю, детали вроде типа доступа не включены. Построил представление,
# которое мог бы использовать прямо сейчас в Нирване.
class DataPage:
    # В Нирване, в качестве data page рассматриваем совокупность output-ов кубика.
    # В id можно положить ссылку на кубик отработавшего графа.
    id: str
    source: DataPageSource


class Object(abc.ABC):
    def get_name(self) -> str:
        return type(self).__name__


# Материализация объекта – процесс представления его в виде, подходящем для передачи в хранилище.
# В этот момент у него появляется id, timestamp создания и т.д.
def materialize_object(obj: Object, data_page: DataPage) -> ObjectDAO:
    pass


def dematerialize_object(obj_dao: ObjectDAO) -> Object:
    pass


def generate_data_page(new_objs: List[Object], new_relations: List[Relation]) -> DataPage:
    # Новые object и relation материализуются. Relation может быть между уже материализованным
    # объектом (полученным из другого DataPage) и новым, это учитывается.
    pass


def create_relation(source_obj: Object, target_obj: Object, type: RelationType = RelationType.STRONG) -> Relation:
    pass
