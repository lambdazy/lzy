from typing import List

from collections import defaultdict

from examples.asr.relations import DataPage, RelationType, Object, create_relation, generate_data_page
from examples.asr.asr_model import Audio, TranscriptMarkup, Assignment, Text, Dict

# Операция по запуску и получению разметки аудио. Операции всегда должны возвращать DataPage.
def markup_audios(
        audios: List[Audio],  # эти аудио поступили на вход операции, т.е. уже были материализованы
) -> DataPage:
    audio_urls = [audio.s3_url for audio in audios]
    assignments = create_toloka_pool_and_get_results(audio_urls)
    markups = []
    relations = []
    for assignment in assignments:
        for task, solution in zip(assignment.tasks, assignment.solutions):
            markup = convert_to_transcript_markup(task, solution)
            markups.append(markup)
            audio = find_audio_by_url(audios, markup)
            # Relation явно создается между нужными объектами.
            relations.append(create_relation(assignment, markup))
            relations.append(create_relation(audio, markup))
            relations.append(create_relation(audio, assignment, type=RelationType.WEAK))
    # Создание data page с новыми objects и relations.
    return generate_data_page(new_objs=assignments + markups, new_relations=relations)


def create_toloka_pool_and_get_results(audio_urls: List[str]) -> List[Assignment]:
    pass


def convert_to_transcript_markup(task: dict, solution: dict) -> TranscriptMarkup:
    pass


def find_audio_by_url(audios: List[Audio], markup: TranscriptMarkup) -> Audio:
    pass


# Выбираем из разметки лучший текст для аудио, сохраняем как ground truth.
def generate_texts_from_markup(audios: List[Audio], markups: List[TranscriptMarkup]) -> DataPage:
    audio_to_markups = defaultdict(list)
    for markup in markups:
        audio = find_audio_by_url(audios, markup)
        audio_to_markups[audio].append(markup)
    texts = []
    relations = []
    for audio, markups in audio_to_markups.items():
        best_markup = choose_best_markup_for_audio(markups)
        audio_text = Text(text=best_markup.text)
        texts.append(audio_text)
        for markup in markups:
            relations.append(create_relation(markup, audio_text, type=RelationType.WEAK))
        relations.append(create_relation(best_markup, audio_text))
        relations.append(create_relation(audio, audio_text))
    return generate_data_page(new_objs=texts, new_relations=relations)


def choose_best_markup_for_audio(markups: List[TranscriptMarkup]) -> TranscriptMarkup:
    pass


# Операция наговора одного текста, как иллюстрация гибкости relation-ов, в этот раз ребро идет в другую сторону.
# Кроме того, добавился новый источник аудио. Изменений в объекте Audio это не потребовало.
class AudioSourceVoiceRecorder(Object):
    def get_name(self) -> str:
        return 'audio-source_voice-recorder'


def record_audio_for_text(text: Text) -> DataPage:
    audio_url = send_text_to_toloka(text)
    audio = Audio(s3_url=audio_url)
    source = AudioSourceVoiceRecorder()
    relations = [
        create_relation(text, audio),
        create_relation(source, audio),
    ]
    return generate_data_page(new_objs=[audio, source], new_relations=relations)


def send_text_to_toloka(text: str) -> str:
    pass


# Решили ввести новые шаги по разметке, включая проверку расшифровок.
# Старые объекты разметок и страниц заданий остаются без relation-ов, миграций не требуется.
class MarkupSession(Object):
    params: dict

    def get_name(self) -> str:
        return 'markup-session'


class MarkupStep(Object):
    name: str

    def get_name(self) -> str:
        return 'markup-step'


# Данные об одном выполненном некоторым асессором задании конкретного типа.
class CheckTranscriptMarkup(Object):
    audio_url: str
    text: str
    ok: bool
    comment: str

    def get_name(self) -> str:
        return 'check-transcript_markup'


def run_check_asr(markup_session: MarkupSession, audio_to_asr_hypothesis: Dict[Audio, str]) -> DataPage:
    assignments = create_toloka_pool_and_get_results(audio_to_asr_hypothesis)
    markups = []
    relations = []
    markup_step = MarkupStep(name='check-asr')
    relations.append(create_relation(markup_session, markup_step))
    for assignment in assignments:
        for task, solution in zip(assignment.tasks, assignment.solutions):
            markup = convert_to_check_transcript_markup(task, solution)
            markups.append(markup)
            audio = find_audio_by_url(list(audio_to_asr_hypothesis.keys()), markup)
            relations.append(create_relation(markup_step, assignment))
            relations.append(create_relation(assignment, markup))
            relations.append(create_relation(audio, markup))
            relations.append(create_relation(audio, assignment, type=RelationType.WEAK))
    return generate_data_page(new_objs=assignments + markups + [markup_step], new_relations=relations)


def create_toloka_pool_and_get_results(audio_to_asr_hypothesis: Dict[Audio, str]) -> List[Assignment]:
    pass


def convert_to_check_transcript_markup(task: dict, solution: dict) -> CheckTranscriptMarkup:
    pass