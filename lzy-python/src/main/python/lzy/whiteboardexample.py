from dataclasses import dataclass
from typing import List, Iterator

from _collections import defaultdict

from lzy.api import op, LzyEnv


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


@dataclass
class WhiteboardExample:
    texts: List[str] = None
    counters: List[Counter] = None


class Result:
    def __init__(self, wb: WhiteboardExample):
        self._texts = wb.texts

    def texts(self):
        return self._texts


def main():
    wb = WhiteboardExample()
    env = LzyEnv(whiteboard=wb)
    with env:
        texts = list(text_source())
        wb.texts = texts
        words = text2words(texts)
        counters = words2count(words)
        wb.counters = list(counters)
    for counter in wb.counters:
        print(counter.word, counter.count)

    for page in env.whiteboards(WhiteboardExample):
        for counter in page.counters:
            print(counter.word, counter.count)

    for page in env.projections(Result):
        for text in page.texts():
            print(text)


if __name__ == "__main__":
    main()
