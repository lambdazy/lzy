from dataclasses import dataclass
from typing import Iterator

from _collections import defaultdict

from lzy.api import op, KeyedIteratorBus, LzyUtils, LzyEnv


@dataclass
class Text:
    text: str


@dataclass
class Word:
    word: str


@dataclass
class Counter:
    word: str
    count: int


@op
def text_source() -> Iterator[Text]:
    for i in range(100):
        yield Text(text="Are you interested in using a neural network to generate text?")


@op
def text2words(texts: Iterator[Text]) -> Iterator[Word]:
    for text in texts:
        for word in text.text.split(' '):
            yield Word(word=word)


@op
def words2count(words: Iterator[Word]) -> Iterator[Counter]:
    counter = defaultdict(int)
    for word in words:
        counter[word.word] += 1
    for k, v in counter.items():
        yield Counter(k, v)


@op
def counter_sink(counters: Iterator[Counter]) -> None:
    for c in counters:
        print(c)


def main():
    env = LzyEnv(local=True, buses=[
        [text2words, KeyedIteratorBus(key_extractor=lambda x: x.word)]
    ])
    with env:
        texts = text_source()
        words = text2words(texts)
        print("Before run:")
        LzyUtils.print_lzy_ops(env.registered_ops())
        env.run()
        print("After run:")
        LzyUtils.print_lzy_ops(env.registered_ops())
        counters = words2count(words)
        counter_sink(counters)
        print("After all calls:")
        LzyUtils.print_lzy_ops(env.registered_ops())
    print("After all runs:")
    LzyUtils.print_lzy_ops(env.registered_ops())


if __name__ == "__main__":
    main()
