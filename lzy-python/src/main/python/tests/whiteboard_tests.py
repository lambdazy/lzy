from unittest import TestCase

from api import print_lzy_ops
from lzy.api import op, LzyEnv
from whiteboard.wb import WhiteBoard


class WhiteboardTests(TestCase):
    # def test_simple_whiteboard(self):
    #     # Arrange
    #     @dataclass
    #     class Text:
    #         text: str
    #
    #     @dataclass
    #     class Word:
    #         word: str
    #
    #     @dataclass
    #     class Counter:
    #         word: str
    #         count: int
    #
    #     @op
    #     def text_source() -> Iterator[Text]:
    #         for i in range(100):
    #             yield Text(text="Are you interested in using a neural network to generate text?")
    #
    #     @op
    #     def text2words(texts: Iterator[Text]) -> Iterator[Word]:
    #         for text in texts:
    #             for word in text.text.split(' '):
    #                 yield Word(word=word)
    #
    #     @op
    #     def words2count(words: Iterator[Word]) -> Iterator[Counter]:
    #         counter = defaultdict(int)
    #         for word in words:
    #             counter[word.word] += 1
    #         for k, v in counter.items():
    #             yield Counter(k, v)
    #
    #     @dataclass
    #     class WhiteboardExample:
    #         texts: List[str] = None
    #         counters: List[Counter] = None
    #
    #     class Result:
    #         def __init__(self, wb: WhiteboardExample):
    #             self._texts = wb.texts
    #
    #         def texts(self):
    #             return self._texts
    #
    #     # Act
    #     whiteboard = WhiteboardExample()
    #     env = LzyEnv(whiteboard=whiteboard, local=True)
    #     with env:
    #         whiteboard.texts = list(text_source())
    #         w = text2words(whiteboard.texts)
    #         counters = words2count(w)
    #         whiteboard.counters = list(counters)
    #
    #     # Assert
    #     for page in env.whiteboards(WhiteboardExample):
    #         self.assertEqual(11, len(page.counters))
    #     for page in env.projections(Result):
    #         for t in page.texts():
    #             self.assertEqual("Are you interested in using a neural network to generate text?", t.text)

    # def test_new_wb(self):
    #     # @op(input_types=(str, str), output_type=str)
    #     @op
    #     def a(p1: str, p2: str) -> str:
    #         return str(p1) + str(p2)
    #
    #     # @op(input_types=(str, str), output_type=str)
    #     @op
    #     def b(p1: str, p2: str) -> str:
    #         return str(p1) + str(p2)
    #
    #     # @op(input_types=(), output_type=str)
    #     @op
    #     def c() -> str:
    #         return 'c'
    #
    #     # @op(input_types=(), output_type=str)
    #     @op
    #     def d() -> str:
    #         return 'd'
    #
    #     # @op(input_types=(str, str), output_type=str)
    #     @op
    #     def e(p1: str, p2: str) -> str:
    #         return str(p1) + str(p2)
    #
    #     # @op(input_types=(), output_type=str)
    #     @op
    #     def g() -> str:
    #         return 'g'
    #
    #     @whiteboard
    #     class WB:
    #         pass
    #         # a: str = None
    #         # b: str = None
    #         # c: str = None
    #         # g: str = None
    #
    #     wb = WB()
    #     env = LzyEnv(whiteboard=wb, local=True)
    #     with env:
    #         wb.g = g()
    #         wb.d = d()
    #         wb.c = c()
    #         wb.b = b(wb.c.val, wb.d.val)
    #         e1 = e(wb.b.val, wb.g.val)
    #         wb.a = a(e1, wb.b.val)
    #         print_lzy_ops(env.registered_ops())
    #     self.assertEqual({'b', 'c', 'd', 'g'}, wb.a.deps)
    #     self.assertEqual({'c', 'd'}, wb.b.deps)

    def test_dependencies(self):
        # @op(input_types=(str, str), output_type=str)
        @op
        def a(p1: str, p2: str) -> str:
            return str(p1) + str(p2)

        # @op(input_types=(str, str), output_type=str)
        @op
        def b(p1: str, p2: str) -> str:
            return str(p1) + str(p2)

        # @op(input_types=(), output_type=str)
        @op
        def c() -> str:
            return 'c'

        # @op(input_types=(), output_type=str)
        @op
        def d() -> str:
            return 'd'

        # @op(input_types=(str, str), output_type=str)
        @op
        def e(p1: str, p2: str) -> str:
            return str(p1) + str(p2)

        # @op(input_types=(), output_type=str)
        @op
        def g() -> str:
            return 'g'

        wb = WhiteBoard()
        env = LzyEnv(whiteboard=wb, local=True)
        with env:
            wb.g = g()
            wb.d = d()
            wb.c = c()
            print(wb.deps)
            wb.b = b(wb.c, wb.d)
            e1 = e(wb.b, wb.g)
            print(wb.deps)
            wb.a = a(e1, wb.b)
            print_lzy_ops(env.registered_ops())

        print(wb.deps)
        self.assertEqual({'b', 'c', 'd', 'g'}, wb.deps['a'])
        self.assertEqual({'c', 'd'}, wb.deps['b'])
