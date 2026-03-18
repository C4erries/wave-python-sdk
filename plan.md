# План создания `wave-python-sdk`

## Кратко
- Сделать полноценный Python SDK для `wave-mq`, который работает по custom binary protocol `wave-mq` поверх TCP и не зависит от `mbctl`
- Клиент знает только seed broker address (`host:port`) и не зависит от того, где запущен брокер: локально, в Docker или на удаленном сервере.
- `mbctl` остаётся CLI-инструментом для ручных проверок, а `wave-python-sdk` становится библиотекой для приложений.
- Первая версия SDK синхронная, без async API, без HTTP и без MQTT.
- В публичном API сразу заложить именованный параметр выбора transport-варианта подключения, но реализовать пока только TCP и сделать его стандартом.

## Публичный API SDK
- Пакет: `wavemq`
- Основной клиент: `WaveMQClient`
- Создание клиента:
  - `WaveMQClient(broker: str, timeout: float = 10.0, auto_route: bool = True, metadata_ttl: float = 30.0, transport: str = "tcp")`
  - `broker` всегда в формате `host:port`
  - `transport` сделать только именованным параметром; positional usage не поддерживать
  - допустимое значение в v1: только `"tcp"`
  - если передан неизвестный transport, клиент должен сразу бросать `ValueError` на этапе создания
- Клиент поддерживает context manager:
  - `with WaveMQClient("mq.example.com:7912") as client: ...`
- Публичные методы:
  - `ping() -> PingResult`
  - `create_topic(name: str, partitions: int = 1, replication_factor: int = 1) -> CreateTopicResult`
  - `produce(topic: str, partition: int, values: Sequence[bytes | str], key: bytes | str | None = None) -> ProduceResult`
  - `fetch(topic: str, partition: int, offset: int = 0, max_bytes: int = 1 << 20) -> FetchResult`
  - `metadata(topics: Sequence[str] | None = None) -> MetadataResult`
  - `list_offsets(topic: str, partition: int) -> ListOffsetsResult`
  - `commit_offset(group: str, topic: str, partition: int, offset: int) -> CommitOffsetResult`
  - `fetch_committed(group: str, topic: str, partition: int) -> FetchCommittedResult`
  - `close() -> None`
- Возвращаемые объекты реализовать как `dataclass`-модели, а не как словари.
- Для `produce(...)` строки автоматически кодировать в UTF-8; `fetch(...)` всегда возвращает `bytes` в `Record.key` и `Record.value`.

## Ошибки и контракт поведения
- Базовое исключение: `WaveMQError`
- Подтипы:
  - `WaveMQConnectionError`
  - `WaveMQProtocolError`
  - `WaveMQBrokerError`
  - `TopicNotFoundError`
  - `PartitionNotFoundError`
  - `TopicExistsError`
  - `NotLeaderError`
  - `InvalidRequestError`
- Ошибки брокера маппить из `wave-mq` error codes в typed exceptions.
- Все exceptions должны содержать:
  - `message`
  - `broker`
  - `error_code` если ошибка пришла от брокера
- SDK не должен требовать от пользователя знания Docker/localhost/internal container names.
- Если включён `auto_route=True`, то на `NotLeaderError` SDK:
  - запрашивает metadata по topic
  - находит leader для нужной partition
  - повторяет запрос ровно один раз к leader
  - если leader не найден или адрес недостижим, бросает исключение
- Важно: для multi-broker сценариев SDK предполагает, что broker metadata содержит reachable advertised addresses; исправление сетевой конфигурации кластера не является задачей SDK.

## Устройство пакета
- Структура репозитория:
  - `pyproject.toml`
  - `README.md`
  - `plan.md`
  - `src/wavemq/__init__.py`
  - `src/wavemq/client.py`
  - `src/wavemq/protocol.py`
  - `src/wavemq/models.py`
  - `src/wavemq/errors.py`
  - `src/wavemq/routing.py`
  - `tests/unit/`
  - `tests/integration/`
- Packaging:
  - использовать `pyproject.toml`
  - backend: `hatchling`
  - Python target: `>=3.10`
  - runtime dependencies: без обязательных внешних зависимостей, только stdlib
- `protocol.py` отвечает только за frame encoding/decoding и wire-format запросов/ответов.
- `client.py` отвечает за публичный API, transport lifecycle и exception mapping.
- В `client.py` сразу заложить transport-dispatch по строковому параметру `transport`, даже если пока есть только одна реализация.
- `routing.py` отвечает за metadata cache, leader lookup и single-retry re-route.
- `models.py` содержит dataclass-модели:
  - `PingResult`
  - `CreateTopicResult`
  - `ProduceResult`
  - `Record`
  - `FetchResult`
  - `PartitionMetadata`
  - `MetadataResult`
  - `ListOffsetsResult`
  - `CommitOffsetResult`
  - `FetchCommittedResult`
- `errors.py` содержит иерархию исключений и mapping broker error code -> exception class.

## Transport и wire protocol
- SDK не должен shell-out в `mbctl`; он реализует клиентский binary protocol напрямую.
- Транспортный контракт SDK:
  - пользователь выбирает transport через `transport=...` при создании `WaveMQClient`
  - по умолчанию использовать `transport="tcp"`
  - transport selection является частью публичного API уже в v1, чтобы дальше можно было расширять без ломающего изменения конструктора
- Transport v1:
  - реализовать только custom TCP transport для binary protocol `wave-mq`
  - один TCP connection на один request
  - без connection pooling
  - timeout задаётся через `timeout`
- Другие transport-варианты в v1 не реализовывать и не документировать как доступные; параметр нужен только как заранее заложенная точка расширения API.
- Correlation ID генерировать внутри клиента монотонно.
- Версия protocol: текущая `version=0`
- Источник истины по wire-format:
  - `wave-mq/internal/netproto/codec.go`
  - `wave-mq/pkg/api/api.go`
- SDK не импортирует Go-код и не зависит от репозитория `wave-mq` на runtime; Go-репозиторий используется только как reference при разработке и тестах.

## Scope первой версии
- Входит:
  - sync client API
  - constructor-level transport selection через `transport=...`
  - custom binary protocol `wave-mq` поверх TCP как единственная реализация transport в v1
  - all current binary operations из `mbctl`
  - typed models
  - broker error mapping
  - metadata-based single retry on `NotLeaderError`
  - basic metadata cache с TTL
- Не входит:
  - async API
  - batching beyond existing `produce(values=[...])`
  - connection pooling
  - consumer abstraction
  - auto background refresh metadata
  - MQTT support
  - HTTP admin API

## Тесты и приемка
- Unit tests:
  - constructor rejects unsupported `transport`
  - default constructor uses `transport="tcp"`
  - encoding/decoding всех request/response payloads
  - mapping всех broker error codes
  - UTF-8 input handling для `str`
  - `bytes` passthrough без порчи payload
  - metadata cache hit/miss/expiry
  - re-route on `NotLeaderError`
  - no extra retry if `auto_route=False`
- Integration tests:
  - single-node broker: `ping/create_topic/produce/fetch/list_offsets/commit/fetch_committed`
  - multi-broker broker: `produce/fetch` через non-leader seed address и автоматический re-route на leader
  - restart-safe smoke: metadata refresh после reconnect
- Acceptance criteria:
  - пользователь может создать `WaveMQClient("host:port")` и работать без `mbctl`
  - пользователь может явно указать `transport="tcp"` и получить тот же результат
  - SDK работает одинаково для локального брокера, Docker с published port и удаленного сервера
  - публичный API возвращает typed objects, а не raw dict/tuple payloads
  - `NotLeaderError` в обычном случае скрыт внутри SDK, если `auto_route=True`

## Документация и примеры
- В `README.md` добавить:
  - quickstart
  - installation
  - supported operations
  - sync usage examples
  - note про reachable advertised addresses в multi-broker режиме
- Добавить минимальные примеры:
  - `examples/basic.py`
  - `examples/produce_fetch.py`
  - `examples/offsets.py`
- Примеры должны использовать только SDK, без `mbctl`.

## Предположения и выбранные дефолты
- SDK ориентирован на Python как основной библиотечный клиент для `wave-mq`.
- Первая версия pure Python, без C extensions и без `librdkafka`-подобных зависимостей.
- Поддерживается только custom binary protocol `wave-mq`; Kafka protocol compatibility не входит в этот репозиторий.
- Параметр `transport` добавляется заранее как API-точка расширения, но фактически поддерживаемый вариант в первой версии только один: TCP.
- Для cluster routing seed address может быть любым reachable broker, но advertised addresses в metadata должны быть доступны из сети клиента.
