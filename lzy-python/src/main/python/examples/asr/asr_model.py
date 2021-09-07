from examples.asr.relations import Object
from typing import List, Tuple, Dict



class AudioSourceCloud(Object):
    folder_id: str
    recognition_mode: str

    def get_name(self) -> str:
        return 'audio-source_cloud'


class AudioSourceImport(Object):
    yt_table_path: str

    def get_name(self) -> str:
        return 'audio-source_import'


class Audio(Object):
    # У объектов нет id и времени создания, они генерируются в процессе материализации (создания DataPage)
    s3_url: str

    def get_name(self) -> str:
        return 'audio'


class Tag(Object):
    value: str

    def get_name(self) -> str:
        return 'tag'


# Станица заданий, задания произвольного вида.
class Assignment(Object):
    user_id: str
    tasks: List[dict]
    solutions: List[dict]

    def get_name(self) -> str:
        return 'assignment'

# Данные об одном выполненном некоторым асессором задании конкретного типа.
class TranscriptMarkup(Object):
    audio_url: str
    text: str
    comment: str

    def get_name(self) -> str:
        return 'transcript_markup'

class Text(Object):
    text: str

    def get_name(self) -> str:
        return 'text'