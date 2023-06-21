В чем у нас проблема:
1) У нас растет количество параметров окружения: добавили exclude_packages, pip_index_url, а уже видно, что это не конец: будут additional_index_url, будут disk_io, network_policy, и проч.
2) Вместе с ними растет количество несовместимых друг с другом комбинаций параметров, усложняется валидация, ад, смерть. Сейчас уже сложно сказать, как именно работает опция local_paths, например.
3) У нас абсолюбно неявная механика мерджа параметров окружения с разных уровней: операция, воркфлоу, лизи. Например, local_modules_paths, libraries и env_variables объединяются с разных уровней, но при этом остальные опции - перезаписываются. Также часть параметров можно установить только на workflow, что тоже не слишком очевидно.

Отсюда хочется получить стройную, непротиворечивую, а главное понятную и документированную систему описывания параметров окружения, которая будет решать проблему несовместимости параметров посредством разных реализаций, нежели plain валидацией, которую еще нужно обмазывать тестами.

Мы имеем 4 типа настройки окружения пользовательской операции, которые конечным образом уезжают на сервер:
1) Ресурсы (Provisioning);
2) Файлы, которые мы тащим с собой, например, локальный проект;
3) Библиотеки, которые мы должны поставить на удаленной машине;
4) Контейнер, который в котором мы запустимся;

Отсюда мы имеем 4 естественных и семантичных точки для настроек окружения со стороны пользователя.
Однако, мы не хотим давать возможность настраивать файлы, которые мы тащим с собой:
1) Локальный и editable-пакеты мы и так хорошо детектим;
2) Файлы-ресурсы стоит отправлять через механизм файлов ресурсов.

Поэтому, мы выделяем три (с половиной) точки-области настройки окружения, которыми может управлять пользователь:

1) Provisioning
2) PythonEnv
3) Container
3.5) Environ

Требования к этим точкам следующие: любые настройки внутри одной точки не должны уметь конфликтовать с другими точками.

Далее, для каждой "точки" мы вводим по абстрактному классу, каждый из которых определяет "артефакт" результата, то есть, например, любой PythonEnv должен обладать `python_version`.
Реализации же будут отличатсья только пользовательским интерфейсом и его преобразованием в "артефакт".


# Интерфейсы классов

```python
class Provisioning:
    cpu_count: int
    ...
    # ничего нового тут


class PythonEnv:
    python_version: str
    local_modules_dirs: List[str]
    pypi_packages: Dict[str, str]
    pip_index_url: str


class ContainerEnv:
    @abstractproperty
    def container_type(self) -> str:
        # now we support only docker, so somewhere will be `if container_env.container_type != 'docker': raise`

```

# Примеры реализаций классов

## 1) Удаленное окружение питона.

```python
# Просто перечисляем нужные библиотеки для установки
class SimpleLibrariesEnv(PythonEnv):
    python_version: str
    pypi_index_url: str
    pypi_packages: Dict[str, str]
    local_modules_dirs: List[str]


# То, что есть сейчас
class AutoLibrariesEnv(PythonEnv):
    #: and we can document
    python_version: str = field(defaultfactory=...)

    #: every option
    pypi_index_url: str

    #: one by one
    additional_pypi_libraries: Dict[str, str]

    #: if we will use sphinx: https://stackoverflow.com/a/66168767
    auto_env_explorer_class: Type[AutoEnvExplorer]

    # maybe additional options, im not sure yet
    raise_on_binaries: bool = True
    treat_editables_as_locals: bool = True

```

Если удаленно не нужно делать вообще ничего, нужно указать `OnlyLocalEnv`

Во всех остальных случаях мы обязаны запускать конду на удаленной машине, даже в докере
и если в докере конды нет - падать с ошибкой про то, что у тебя указано непустое окружение,
а конды в контейнере нет (возможно, стоит это дело инкапсулировать в свой пакет `lzy_env_manager`,
чтобы не торчать наружу кондой).

*Игорь говорит/предлагает, что при отсутствии конды в докере нужно сделать фоллбек на pip*

```python
class OnlyLocalEnv(PythonEnv):
    pypi_index_url: str
    auto_env_explorer_class: Type[AutoEnvExplorer]

    @property
    def libraries(self):
        return {}  # empty

```

## 2) Настройки контейнера

```python
class DockerEnv(ContainerEnv):
    registry: str
    image: str
    pull_policy: DockerPullPolcicy  # надо бы разобраться, что это и нафига это

    # Надо бы научиться использовать credentials store и локальные конфиги докера
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
    python_env: PythonEnv
    container: ContainerEnv
```

Далее, мы говорим, что каждая опа и каждый WF имеет свой `Env`, и возможно даже объект `Lzy`
(чтобы дать возможность настроить окружение один раз на всю программу).

Переопределение идет op >> wf >> lzy env, то есть окружение, указанное на op-е будет самым приоритетным
под-env-ы (`python_env`, `container`) больше не мерджатся, потому что они могут быть разного типа,
а если мерджить их при совпадении типа - получаем неразбериху.

Возможно, Provisioning можно мерджить, потому что он не предполагает наследования.

Если на опе будет указан `provisioning` и только он, а на wf будет указан
`docker_env`, то в результирующую опу прилетит и `provisioning` и `docker_env`.


# Про `pypi_index_url` и explore пакетов

Внутри `python_env` у нас будет сущность `AutoEnvExplorer`, который является сущностью, замкнутой над
`pypi_index_url` и `exclude_packages`:


```python
class AutoEnvExplorer:
    pypi_index_url: str
    exclude_packages: List[str]

    def get_local_packages_dirs(self) -> List[str]:
        pass

    def get_pypi_installed_libraries(self) -> Dict[str, str]:
        pass
```

При этом пользователь нигде явно его не задает, кроме как:


class AutoPythonEnv:
    pypi_index_url: str
    additional_pypi_libraries: Dict[str, str]
    auto_env_explorer_class: Type[AutoEnvExplorer]

    def __post_init__(self):
        # на самом деле вместо auto_env_explorer_class нужна factory,
        # у которой мы будем смотреть аргументы, чтобы дать возможность
        # создавать свои PythonEnv и Explorer с разными зависимостями по аргументам
        self.auto_env_explorer = self.auto_env_explorer_class(
            pypi_index_url=self.pypi_index_url,
            exclude_packages=self.additional_pypi_libraries.keys()
        )

    @property
    def pypi_libraries(self):
        return self.auto_env_explorer.get_pypi_installed_libraries() + self.additional_pypi_libraries

    @property
    def local_modules_dirs(self):
        return self.auto_env_explorer.get_local_packages_dirs()


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

    def with_python_env(
        self, *,
        klass: Type[FilesEnv] = AutoPythonEnv,
        **kwargs
    ) -> ...:
        # Также пускай klass умеет принимать строки типа "lzy.api.v1.env.AutoPythonEnv",
        # и импортирует такой объект, если пользователь не хочет писать импорт сверху.

    def with_container(...) -> ...:
        pass

    def with_env(sels, env: Env = None, *, provisioning: Provisioning = None, python_env: PythonEnv = None, ...) -> ...:
        # фабричный метод, чтобы не вызывать предыдущие четыре по-одному

    # фантазия на будущее
    @classmethod
    def generate_arparser(cls):
        ...

    @classmethod
    def from_cli_args(cls, argv=sys.argv) -> ...:
        ...

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
        .with_python_env(
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
      .with_python_env(...,
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
    .with_python_env(
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
def foo():
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

Альтернативный возможный способ настройки через декораторы и `__call__ (подходит только для @op):

```python
@Provisioning(cpu=123)
@AutoFilesEnv(local_paths=[...])
@op()
def foo():
    pass
```

Альтернативный способ настройки через `__enter__`, но он подойдет только для wf и я не хочу его делать:

```python
with lzy.workflow("name") as wf, Provisioning(cpu_count=2):
    pass
```

Предложения про другие варианты настройки енвов приветствуются.


# Про организацию пакетов и импортов

```
lzy.env.environment
lzy.env.python.base
lzy.env.python.auto
lzy.env.python.easy
lzy.env.python.local
lzy.env.container.base
lzy.env.container.docker
lzy.env.explorer.base
lzy.env.explorer.auto
```

А также в `lzy.api.v1.env` делаем реимпорт всех нужных символов из реализации выше.
С помощью реимпортов имеем чистое и ограниченное API у модуля, в котором автоподсказки не подсказывают ненужный треш.

Использование предполагается следующее (подаем пример в документациях):

```
from lzy.api.v1 import op, envs as lzenv

@lzenv.Provisioning(...)
@op
def function(...):
   pass
```
