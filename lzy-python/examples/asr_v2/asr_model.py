from dataclasses import dataclass

from examples.asr_v2.relations import DataPage
from typing import List, Tuple, Dict, TypeVar, Generic
from lzy.api import op, data_page

class TolokaPool:
    def __init__(self):
        pass


@dataclass
class Record:
    url: str


@dataclass
class RecordMarkup:
    record_url: str
    text: str


@dataclass
class TranscriptionWhiteboard:
    records: List[Record]
    splits: Dict[Record, str]
    anonymized_splits: Dict[Record, str]

    silence_pool: TolokaPool
    silence_fragments: Dict[str, Tuple[str, bool]]

    asr_results: Dict[str, str]

    transcription_pool: TolokaPool
    transcription_results: Dict[str, str]

    transcriptions: List[RecordMarkup]


@dataclass
class TranscriptionWhiteboardV2(TranscriptionWhiteboard):
    ext: List[RecordMarkup]


class Transcription(DataPage[RecordMarkup]):
    def __init__(self, wb: TranscriptionWhiteboard):
        super().__init__(wb.records)
        self.strong(wb, 'transcriptions')
        self.weak(wb, 'records')


    def splits(self, record_url) -> List[str]:
        return


def transcribe(records: DataPage[Record]) -> Transcription:
    wb = TranscriptionWhiteboard()
    with env = LzyEnv():
        wb.splits = split(records)
        wb.sil_pool = create_silence_pool(wb.splits)
        for i in range(len(wb.splits)):
            ahjgds
        env.run()
    return Transcription(wb)


def transcribe(records: DataPage[Record]) -> Transcription:
    wb = TranscriptionWhiteboard()
    with env = LzyEnv(wb):
        wb.splits = split(records)
        wb.sil_pool = create_silence_pool(wb.splits)
        for i in range(len(wb.splits)):
            ahjgds
        env.run()
    return Transcription(wb)


def analyze1():
    with env = LzyEnv():
        for page in env.pages(TranscriptionWhiteboard):
            wb.splits = split(page)
            wb.sil_pool = create_silence_pool(splits)
        env.run()

def analyze2():
    env = LzyEnv()
    for page in env.pages(Transcription):
        with localEnv = LzyEnv():
            wb.splits = split(page)
            wb.sil_pool = create_silence_pool(splits)
        env.run()

