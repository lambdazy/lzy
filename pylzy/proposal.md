Мы имеем 4 типа настройки окружения пользовательской операции, которые конечным образом уезжают на сервер:
1) Ресурсы (Provisioning)
2) Файлы, которые мы тащим с собой (например, локальный проект)
3) Библиотеки, которые мы должны поставить на удаленной машине
4) Контейнер, который в котором мы запустимся
Отсюда мы имеем 4 естественных и семантичных точки для настроек окружения со стороны пользователя:

# Интерфейсы классов

```python
class Provisioning:
    cpu_count: int
    ...
    # ничего нового тут

# Названия еще предстоит обсудит
class FilesEnv:
    @abstractproperty
    def local_paths(self) -> List[str]:
        pass


class PythonEnv:
    @abstractmethod
    def generate_conda_config(self) -> Dict[str, Any]:
        pass


class ContainerEnv:
    @abstractproperty
    def container_type(self) -> str:
        pass
```

# Примеры реализаций классов

## 2) Окружения локальных файлов:

```python
class ManualFilesEnv(FilesEnv):
    def __init__(self, local_paths: List[str]):
        self.local_paths = local_paths


class AutoFilesEnv(FilesEnv):
    pypi_index_url: str
    additional_paths: str

    def __init__(self, pypi_index_url: str, additional_paths: List[str]):
        self.local_paths = self.explore_local_paths(pypi_index_url) + additional_paths

```

## 3) Удаленное окружение питона.

```python
#Не уверен, что мы так уж хотим полностью поддерживать все возможности конды
class CondaEnv(PythonEnv):
    channels: List[str]
    dependencies: List[str]
    pip_dependencies: List[str]
    pypi_index_url: str

    @classmethod
    def from_conda_yaml(self, path: str) -> CondaEnv:
        # also validate here that there is only allowed options at conda yaml

    def generate_conda_config(self):
        ...

# Просто перечисляем нужные библиотеки для установки
class SimpleLibrariesEnv(PythonEnv):
    python_version: str
    pypi_index_url: str
    pypi_libraries: Dict[str, str]


# То, что есть сейчас
class AutoLibrariesEnv(PythonEnv):
    python_version: str = field(defaultfactory=...)
    exclude_packages: List[str]
    pypi_index_url: str
    additional_pypi_libraries: Dict[str, str]

    def generate_conda_config(self):
        ...
```

Если удаленно не нужно делать вообще ничего, нужно указать `EmptyEnv`

Во всех остальных случаях мы обязаны запускать конду на удаленной машине, даже в докере
и если в докере конды нет - падать с ошибкой про то, что у тебя указано непустое окружение,
а конды в контейнере нет (возможно, стоит это дело инкапсулировать в свой пакет `lzy_env_manager`,
чтобы не торчать наружу кондой)

```python
class EmptyEnv(PythonEnv):
    def generate_conda_config(self):
        return {}
```

## 4) Настройки контейнера

```python
class DockerEnv(ContainerEnv):
    registry: str
    image: str
    pull_policy: DockerPullPolcicy
    # Научиться использовать credentials store и локальные конфиги докера
    username: Optional[str]
    password: Optional[str]
```

# Окружение в целом

Отсюда идет логичное итоговое окружение:

```python
class Env:
    environ: Dict[str, str]

    # Все классы - ABC, чтобы дать возможность наследования
    provisioning: Provisioning
    files: FilesEnv
    python_env: PythonEnv
    container: ContainerEnv
```

Далее, мы говорим, что каждая опа и каждый WF имеет свой `Env`, и возможно даже объект `Lzy`
(чтобы дать возможность настроить окружение один раз на всю программу).

Переопределение идет op >> wf >> lzy env, то есть окружение, указанное на op-е будет самым приоритетным
под-env-ы (`files`, `python_env`, `container`) больше не мерджатся, потому что они могут быть разного типа,
а если мерджить их при совпадении типа - получаем неразбериху.

Возможно, Provisioning можно мерджить, потому что он не предполагает наследования.
Кстати, можно мерджить и под-envы, только не их "настройки",
а уже их артефакты (`local_paths`, `conda_yaml`), но не принесет ли это неразберихи?

Кстати, можно (и возможно нужно) ввести следующий хелпер для мерджа окружений:

```python
class Env:
    provisioning: Union[Provisioning, NotSpecified[Provisioning]] = NotSpecified(default_factory=Provisioning)
    ...
```

В таком случае, если на опе будет указан `provisioning` и только он, а на wf будет указан
`docker_env`, то в результирующую опу прилетит и `provisioning` и `docker_env`.


# Про pypi_index_url и explore пакетов

Есть две проблемы:
* Часть вещей шарятся между разными подокружениями, в частности, `pypi_index_url` должен быть настройкой \
`AutoFilesEnv`, а также всех `PythonEnv`.
* А также, `AutoFilesEnv` и `AutoLibrariesEnv` должны переиспользовать один и тот же код поиска пакетов.

Решение - вводим дополнительную сущность:

```python
class LocalEnvExplorer:
    pypi_index_url: str
    exclude_packages: str

    def explore(self, namespace: Dict[Any]) -> Tuple[LocalPaths, LocalLibraries]:
        pass
```

Проблемы начинаются тогда, когда мы хотим дать пользователю настроить LocalEnvExplorer,
при том, что AutoFilesEnv и PythonEnv должны содержать один и тот же экзмепляр LocalEnvExplorer.

Варианты, которые я вижу:

### 1. Вариант с членством в Env

Сделать `LocalEnvExplorer` членом `Env`, и при итоговом вычислении окружения передавать его
в `AutoFilesEnv` и `PythonEnv`.

Тут проблема в том, что мы должны в общем случае понимать, когда нужно передавать его провайдерам,
а когда - нет (`ManualFilesEnv` в нем не нуждается).

Разве что делать это через инспект интерфейса провайдера, но чет такое себе.

Альтернатива - сделать `LocalEnvExplorer` частью
интерфейса конструктора `FilesEnv и` `PythonEnv`, а наследники пускай сами решают, когда нужно использовать,
а когда - нет, но этот вариант мне тоже не нравится.

### 2. Tornado-Configurable-like

Оформить LocalEnvExplorer [tornado-like способом](https://www.tornadoweb.org/en/stable/util.html#tornado.util.Configurable.configure),
чтобы можно было бы сделать `LocalEnvExplorer.configure(pypi_index_url='...')` и это бы влияло на все новосозданные объекты этого класса.

Плохо, потому что выбивается из остальных вариантов настройки других вещей.

### 3. Идти от валидации

При валидации Env проверять, что все под-env-ы имеют одинаковые настройки `LocalEnvExplorer`, если они есть.
Тогда, когда `AutoFilesEnv` пойдет создавать `LocalEnvExplorer`, он создатся таким же, как и в других местах.

Минус - необходимость держать настройки `LocalEnvExplorer` внутри `AutoFilesEnv` и компании.

### 4. Singleton

Вообще вынести `LocalEnvExplorer` из логики Env-ов, и сделать его псевдо-синглтоном на объекте `Lzy`.
Тогда `pypi_index_url` убирается из настроек `AutoFilesEnv` и `PythonEnv`, а они при своей работе получают
ссылку на единственный `LocalEnvExplorer`.

Плюсы: чище код, простая реализация

Минусы: настройки в `Lzy` может быть неочевидным местом для пользователя, т.к. все пакетное он настраивает
где-нибудь в WF, а тут часть настройки (`pypi_index_url`, например) необходимо настраивать вообще в другом месте.


# Про пользовательский интерфейс настройки наших *Env-ов.

Первым делом - мы убираем все настройки, связанные с нашими Env-ами из сигнатур @op и .workflow:

```python
def op(func, *, output_types, description, version, cache, lazy_arguments):
    pass

def workflow(self, *, eager, interactive):
    pass

```

Вторым делом - говорим, что Lzy, op и workflow - возвращают объекты типа `WithEnvironmentMixin`:

```python
class WithEnvironmentMixin:
    env: Env
```

Первый вариант настройки интерфейса, который я вижу:

```python
class WithEnvironmentMixin(Generic[T]):
    env: Env

    def with_provisioning(
        self, *,
        klass: Type[Provisioning] = Provisioning,
        **kwargs
    ) -> T: # Еще предстоит побороться за правильные type-хинты
        # Метод возвращает клон оригинального объекта, только с подмененным
        # env.provisioning = klass(**kwargs)

    def with_files_env(
        self, *,
        klass: Type[FilesEnv] = AutoFilesEnv,
        **kwargs
    ) -> ...:
        # Также пускай klass умеет принимать строки типа lzy.api.v1.env.AutoFilesEnv,
        # и импортирует такой объект, если пользователь не хочет писать импорт сверху.
        pass

    def with_python_env(...) -> ...:
        pass

    def with_container(...) -> ...:
        pass
```

Это позволит следующие конструкции:

```python
@op()
def foo():
    pass

lzy = lzy.with_provisioning(cpu_count=5)

with lzy.workflow('123').with_files_env(
    AutoLibrariesEnv, pypi_index_url='foo', additional_pypi_libraries=['...']
) as wf:
    result = foo.with_container(...)()  # диковато выглядит
```

Другие варианты сложного форматирования (нужно будет выбрать один и показывать его как пример в доках):

```python
with lzy.workflow('123') \
        .with_files_env(
            ...,
            ...,
        ) \
        .with_container(
            ...,
            ...,
        ) \
        .with_provisioning(
            ...
            ...,
        ) as wf:
    pass

with (lzy.workflow('123')
      .with_files_env(...,
                      ...)
      .with_container(...,
                      ...)
      .with_provisioning(...,
                         ...)) as wf:  # какая дичь
    pass

with (
    lzy.workflow('213')
    .with_container(
        ...,
        ...,
    )
    .with_provisioning(
        ...,
        ...,
    )
    .with_files_env(
        ...,
        ...
    )
) as wf:
    pass


wf = lzy.workflow('123') \
    .with_container(..., ...) \
    .with_provisioning(..., ...) \
    .with_python_env(..., ...)

with wf:
    pass
```

Можно постараться, и сделать возможным

```python
@op().with_python_env(...).with_container(...)
def foo():
    pass
```

Главная проблема такого метода настройки - автоподсказки параметров этих классов идут нафиг ((((

Вероятно, это не отменяет возможности явно настроить окружение:

```python
env = Env(
    python_env=AutoLibrariesEnv(
        pypi_index_url='foo'
    )
)

@op(env=env):
    pass
```

Или так:

```python
env = Env()
env.python_env = AutoLibrariesEnv()
env.python_env.pypi_index_url = 'foo'

with lzy.workflow() as wf:
    wf.env = env  # Так можно сделать, потому что окружение вычисляется в первый ленивый момент
```

Альтернативный возможный способ настройки через декораторы (подходит только для @op):

```python
@Provisioning(cpu=123).set
@AutoFilesEnv(local_paths=[...]).set
@op()
def foo():
    pass
```

Альтернативный способ настройки через `__enter__` (подойдет только для wf):

```python
with lzy.workflow("name") as wf, Provisioning(cpu_count=2):
    pass
```

Еще есть вариант вернуться к старому-доброму tornado.Configurable, но это будет
влиять на все созданные объекты и будет беда с различными окружениями для wf и op

```python
FilesEnv.configure(AutoFilesEnv, pypi_index_url='foo')
files_env = FilesEnv.new() -> AutoFilesEnv
```

Предложения про другие варианты настройки енвов приветствуются.
