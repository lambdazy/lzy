from dataclasses import dataclass
from typing import List, Iterator
from unittest import TestCase

from _collections import defaultdict

from lzy.api import op, LzyEnv


class WhiteboardTests(TestCase):
    def test_simple_whiteboard(self):
        # Arrange
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

        # Act
        whiteboard = WhiteboardExample()
        env = LzyEnv(whiteboard=whiteboard)
        with env:
            t = list(text_source())
            whiteboard.texts = t
            w = text2words(t)
            counters = words2count(w)
            whiteboard.counters = list(counters)

        # Assert
        for page in env.whiteboards(WhiteboardExample):
            self.assertEqual(11, len(page.counters))
        for page in env.projections(Result):
            for t in page.texts():
                self.assertEqual("Are you interested in using a neural network to generate text?", t.text)
