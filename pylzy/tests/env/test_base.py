import pickle
from typing import Dict, Any, List, Set
from dataclasses import dataclass

import pytest

from lzy.env.base import Deconstructible,  NotSpecified, EnvironmentField


@dataclass
class SimpleDeconstructible(Deconstructible):
    simple: str
    complex: Dict[str, Any]
    simple_required: EnvironmentField[int]
    simple_not_specified: EnvironmentField[int] = NotSpecified
    complex_not_specified: EnvironmentField[List[str]] = NotSpecified
    simple_specified_default: EnvironmentField[str] = ''


@pytest.fixture
def simple_deconstructible():
    return SimpleDeconstructible(
        simple='a',
        complex={'a': 1, 'b': 2},
        simple_required=2,
    )


def test_simple_repr(simple_deconstructible: SimpleDeconstructible) -> None:
    simple_repr = repr(simple_deconstructible)
    restored = eval(simple_repr)

    assert restored == simple_deconstructible

    # checking __ne__ because we can
    assert not (restored != simple_deconstructible)


def test_pickle(simple_deconstructible: SimpleDeconstructible) -> None:
    dumped = pickle.dumps(simple_deconstructible)
    loaded = pickle.loads(dumped)

    assert loaded == simple_deconstructible


def test_ordering(simple_deconstructible: SimpleDeconstructible) -> None:
    first = simple_deconstructible
    second = first.with_fields(simple_required=3)
    third = first.with_fields(simple='b')
    third2 = first.with_fields(simple='b')

    assert first != second != third
    assert first != third

    assert third == third2
    assert third is not third2


def test_hash(simple_deconstructible: SimpleDeconstructible) -> None:
    @dataclass(frozen=True)
    class HashableDeconstructible(Deconstructible):
        a: int = 0
        b: EnvironmentField[str] = NotSpecified
        c: Any = 1

    set_: Set[Deconstructible] = set()

    with pytest.raises(TypeError):
        set_.add(simple_deconstructible)

    set_.add(HashableDeconstructible())

    with pytest.raises(TypeError):
        set_.add(HashableDeconstructible(c={}))


def test_combine(simple_deconstructible: SimpleDeconstructible) -> None:
    first = simple_deconstructible
    second = first.with_fields(
        simple_not_specified=3,
        simple_required=4
    )

    assert first.combine(second) == SimpleDeconstructible(
        simple='a',
        complex={'a': 1, 'b': 2},
        simple_required=4,
        simple_not_specified=3
    ) == second

    assert second.combine(first) == SimpleDeconstructible(
        simple='a',
        complex={'a': 1, 'b': 2},
        simple_required=2,
        simple_not_specified=3
    ) == simple_deconstructible.with_fields(simple_not_specified=3)
