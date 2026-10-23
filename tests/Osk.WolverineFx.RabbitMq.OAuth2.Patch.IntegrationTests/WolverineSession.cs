using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.OAuth2;
using Wolverine;
using Wolverine.RabbitMQ;
using Wolverine.RabbitMQ.Internal;
using Wolverine.Runtime;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Patch.IntegrationTests;

/// <summary>
/// Изолирует host Wolverine, AMQP-ресурсы, credentials и ожидания доставки одного интеграционного теста.
/// </summary>
/// <remarks>
/// Использует общее контейнерное окружение, но собственные имена очереди и соединений. Проверки DI lifetime включены во всех окружениях запуска.
/// </remarks>
internal sealed class WolverineSession : IAsyncDisposable
{
    private readonly IOAuth2Client _oauthClient;
    private readonly HttpClientHandler _httpHandler;
    private readonly OAuth2Environment _environment;
    private readonly IHost _host;
    private readonly DeliveryProbe _delivery = new();
    private long _deliveredMessages;
    private bool _started;
    private Task? _disposeTask;
    /// <summary>
    /// Уникальное клиентское имя обоих AMQP-соединений этой сессии.
    /// </summary>
    public string ConnectionName { get; } = $"wolverine-oauth2-it-{Guid.NewGuid():N}";
    /// <summary>
    /// Уникальная очередь, которую создаёт и использует только эта сессия.
    /// </summary>
    public string QueueName { get; } = $"oauth2-it-{Guid.NewGuid():N}";
    /// <summary>
    /// Provider настоящих JWT с диагностическими счётчиками и управляемым отказом.
    /// </summary>
    public TrackingCredentialsProvider Credentials { get; }
    /// <summary>
    /// Локальные потокобезопасные счётчики успешных и неуспешных обновлений токена.
    /// </summary>
    public RefreshLog Log { get; } = new();

    /// <summary>
    /// Настраивает отдельный host с собственными маршрутизацией, provider и обработчиком сообщений.
    /// </summary>
    /// <param name="environment">Общее запущенное окружение с контейнерами.</param>
    /// <param name="oauthClient">OAuth2-клиент, принадлежащий создаваемой сессии.</param>
    /// <param name="httpHandler">Принадлежащий сессии обработчик HTTPS с доверием тестовому CA.</param>
    /// <param name="invalidToken">Признак изменения подписи выдаваемого JWT для отрицательной проверки.</param>
    private WolverineSession(OAuth2Environment environment, IOAuth2Client oauthClient, HttpClientHandler httpHandler, bool invalidToken)
    {
        _oauthClient = oauthClient;
        _environment = environment;
        _httpHandler = httpHandler;
        Credentials = new TrackingCredentialsProvider(
            new OAuth2ClientCredentialsProvider(ConnectionName, oauthClient), invalidToken);
        _host = Host.CreateDefaultBuilder()
            .UseDefaultServiceProvider(options =>
            {
                options.ValidateScopes = true;
                options.ValidateOnBuild = true;
            })
            .ConfigureLogging(logging => logging.ClearProviders().SetMinimumLevel(LogLevel.Debug).AddProvider(Log))
            .UseWolverine(options =>
            {
                options.UseRuntimeCompilation();
                options.Services.AddSingleton(_delivery);
                options.Discovery.DisableConventionalDiscovery();
                options.Discovery.IncludeType<ProbeMessageHandler>();
                // Invalid credentials stay invalid: fail after the first real broker
                // rejection instead of Wolverine's twenty startup attempts, five seconds apart.
                if (invalidToken) options.BrokerInitializationTimeout = TimeSpan.Zero;
                options.UseRabbitMq(factory =>
                {
                    factory.HostName = environment.RabbitMq.Hostname;
                    factory.Port = environment.RabbitMq.GetMappedPublicPort(5672);
                    factory.CredentialsProvider = Credentials;
                    factory.ClientProvidedName = ConnectionName;
                    factory.RequestedConnectionTimeout = TimeSpan.FromSeconds(10);
                }).AutoProvision();
                options.ListenToRabbitQueue(QueueName);
                options.PublishMessage<ProbeMessage>().ToRabbitQueue(QueueName);
            }).Build();
    }

    /// <summary>
    /// Создаёт OAuth2-клиент и отдельный host для переданного контейнерного окружения.
    /// </summary>
    /// <param name="environment">Запущенное общее окружение Keycloak и RabbitMQ.</param>
    /// <param name="invalidToken">Изменять только подпись JWT для проверки отказа аутентификации.</param>
    /// <param name="ct">Токен отмены создания OAuth2-клиента.</param>
    /// <returns>Новая сессия, которую вызывающий код должен освободить через DisposeAsync.</returns>
    public static async Task<WolverineSession> CreateAsync(OAuth2Environment environment, bool invalidToken = false, CancellationToken ct = default)
    {
        var started = Stopwatch.GetTimestamp();
        var handler = environment.Tls.CreateHandler();
        WolverineSession? session = null;
        try
        {
            var oauth = await new OAuth2ClientBuilder("wolverine", environment.ClientSecret, tokenEndpoint: environment.TokenEndpoint)
                .SetHttpClientHandler(handler).BuildAsync(ct);
            return session = new WolverineSession(environment, oauth, handler, invalidToken);
        }
        catch { handler.Dispose(); throw; }
        finally { WriteTiming("create", started, session?.ConnectionName); }
    }

    /// <summary>
    /// Запускает host и ожидает начальные успешные обновления credentials.
    /// </summary>
    /// <param name="ct">Токен отмены запуска и ожидания.</param>
    /// <returns>Задача запуска сессии и готовности обновления токенов.</returns>
    public async Task StartAsync(CancellationToken ct = default)
    {
        var started = Stopwatch.GetTimestamp();
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(45));
        try
        {
            await _host.StartAsync(timeout.Token);
            _started = true;
            await OAuth2Environment.WaitUntilAsync(() => Task.FromResult(Log.Successes >= 2), TimeSpan.FromSeconds(15), ct);
        }
        finally { WriteTiming("start", started, ConnectionName); }
    }

    /// <summary>
    /// Публикует уникальные сообщения, ожидает их обработку и подтверждает доставку всей группы статистикой RabbitMQ.
    /// </summary>
    /// <remarks>
    /// Scoped IMessageBus получается из отдельного async scope на весь вызов. Вызовы внутри одной сессии должны быть последовательными: используется последний подтверждённый счётчик её уникальной очереди.
    /// </remarks>
    /// <param name="ct">Токен отмены ожиданий доставки и запросов Management API.</param>
    /// <param name="messageCount">Положительное число сообщений в проверяемой группе.</param>
    /// <returns>Задача проверки получения каждого ID и роста broker deliver_get минимум на размер группы.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Число сообщений неположительное.</exception>
    public async Task SendAndReceiveAsync(CancellationToken ct = default, int messageCount = 1)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(messageCount);
        await using var scope = _host.Services.CreateAsyncScope();
        var bus = scope.ServiceProvider.GetRequiredService<IMessageBus>();
        // The unique queue starts at zero and only this session publishes to it.
        // Keep the last observed broker count instead of fetching it again before each send.
        var before = _deliveredMessages;
        for (var i = 0; i < messageCount; i++)
        {
            var id = Guid.NewGuid();
            var received = _delivery.Expect(id);
            await bus.PublishAsync(new ProbeMessage(id));
            await received.WaitAsync(TimeSpan.FromSeconds(15), ct);
        }
        // Prove the whole batch went through RabbitMQ, even if local routing is accidentally enabled.
        await OAuth2Environment.WaitUntilAsync(async () =>
        {
            var delivered = await _environment.DeliveredMessagesAsync(QueueName, ct);
            if (delivered - before < messageCount) return false;
            _deliveredMessages = delivered;
            return true;
        }, TimeSpan.FromSeconds(15), ct);
    }

    /// <summary>
    /// Получает под блокировкой registry ссылки на задачи refresher и соответствующие AMQP-соединения.
    /// </summary>
    /// <remarks>
    /// Диагностика читает приватные поля закреплённой OAuth2 2.0.0 и адаптера; при изменении зависимостей helper нужно пересмотреть. Worker ожидает callback UpdateSecretAsync, поэтому завершённая задача исключает незавершённое обновление.
    /// </remarks>
    /// <returns>Снимок регистраций до начала освобождения транспорта.</returns>
    /// <exception cref="MissingFieldException">Ожидаемое приватное поле отсутствует или не содержит значения.</exception>
    public RefreshDiagnostics[] CaptureRefreshDiagnostics()
    {
        var transport = _host.Services.GetRequiredService<IWolverineRuntime>().Options.Transports.OfType<RabbitMqTransport>().Single();
        var registry = RabbitMqRefreshRegistry.For(transport);
        // OAuth2 2.0.0 exposes no worker completion API. Its worker awaits the
        // refresh callback, including UpdateSecretAsync, so completion also rules
        // out an in-flight secret update. Snapshot every registration under its lock.
        lock (ReadField<Lock>(registry, "_sync"))
        {
            return ReadField<List<RabbitMqCredentialsRefreshRegistration>>(registry, "_refreshers")
                .Select(registration => new RefreshDiagnostics(
                    ReadField<Task>(ReadField<CredentialsRefresher>(registration, "_refresher"), "_refreshTask"),
                    ReadField<IConnection>(registration, "_connection"))).ToArray();
        }
    }

    /// <summary>
    /// Читает обязательное приватное поле для диагностики закреплённой реализации.
    /// </summary>
    /// <typeparam name="T">Ожидаемый тип значения поля.</typeparam>
    /// <param name="instance">Объект с диагностируемым полем.</param>
    /// <param name="name">Точное имя приватного поля.</param>
    /// <returns>Значение поля, приведённое к ожидаемому типу.</returns>
    /// <exception cref="MissingFieldException">Поле отсутствует или содержит null.</exception>
    private static T ReadField<T>(object instance, string name) =>
        (T)(instance.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)?.GetValue(instance)
            ?? throw new MissingFieldException(instance.GetType().FullName, name));

    /// <summary>
    /// Останавливает host и освобождает принадлежащие сессии OAuth2- и HTTP-ресурсы.
    /// </summary>
    /// <returns>Операция завершения работы сессии.</returns>
    public ValueTask DisposeAsync() => new(_disposeTask ??= DisposeCoreAsync());

    /// <summary>
    /// Останавливает host и обеспечивает освобождение credentials provider, OAuth2-клиента и HTTPS handler.
    /// </summary>
    /// <returns>Задача завершения работы и освобождения ресурсов.</returns>
    private async Task DisposeCoreAsync()
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            if (_started)
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                await _host.StopAsync(timeout.Token);
            }
        }
        finally
        {
            try
            {
                if (_host is IAsyncDisposable asyncHost) await asyncHost.DisposeAsync();
                else _host.Dispose();
            }
            finally
            {
                Credentials.Dispose();
                (_oauthClient as IDisposable)?.Dispose();
                _httpHandler.Dispose();
                WriteTiming("dispose", started, ConnectionName);
            }
        }
    }

    /// <summary>
    /// Выводит длительность операции сессии с её диагностическим именем.
    /// </summary>
    /// <param name="stage">Название измеряемой операции.</param>
    /// <param name="started">Начальная метка Stopwatch.</param>
    /// <param name="connectionName">Имя сессии либо null при ошибке её создания.</param>
    private static void WriteTiming(string stage, long started, string? connectionName) =>
        Console.WriteLine($"[timing] session {connectionName ?? "creation failed"} {stage}: {Stopwatch.GetElapsedTime(started).TotalMilliseconds:F0} ms");
}

/// <summary>
/// Хранит ссылки на worker обновления credentials и обслуживаемое им AMQP-соединение.
/// </summary>
/// <param name="Worker">Задача штатного refresher, ожидающая получение credentials и callback обновления токена.</param>
/// <param name="Connection">Соединение, которому callback передаёт новый токен.</param>
internal sealed record RefreshDiagnostics(Task Worker, IConnection Connection);

/// <summary>
/// Добавляет диагностические счётчики, ограничение HTTP-запроса и управляемые ошибки к настоящему OAuth2 provider.
/// </summary>
/// <remarks>
/// Счётчики и флаг отказа потокобезопасны. Значения токенов не записываются в журналы; exp читается только для временных assertions.
/// </remarks>
/// <param name="inner">Provider, который получает JWT у Keycloak.</param>
/// <param name="invalidToken">Изменять подпись полученного JWT, сохраняя payload и срок действия.</param>
internal sealed class TrackingCredentialsProvider(OAuth2ClientCredentialsProvider inner, bool invalidToken) : ICredentialsProvider, IDisposable
{
    private int _requests, _active, _failures, _failTokenRequests;
    private readonly ConcurrentQueue<DateTimeOffset> _expirations = new();
    /// <summary>
    /// Имя исходного OAuth2 provider.
    /// </summary>
    public string Name => inner.Name;
    /// <summary>
    /// Общее число вызовов GetCredentialsAsync, включая ошибки и отмены.
    /// </summary>
    public int Requests => Volatile.Read(ref _requests);
    /// <summary>
    /// Число текущих вызовов, включая ожидание внутренней блокировки provider.
    /// </summary>
    public int ActiveRequests => Volatile.Read(ref _active);
    /// <summary>
    /// Число неуспешных вызовов, включая управляемые ошибки, таймауты и отмены.
    /// </summary>
    public int Failures => Volatile.Read(ref _failures);
    /// <summary>
    /// Потокобезопасный флаг отказа перед обращением к настоящему OAuth2 provider.
    /// </summary>
    public bool FailTokenRequests
    {
        get => Volatile.Read(ref _failTokenRequests) != 0;
        set => Volatile.Write(ref _failTokenRequests, value ? 1 : 0);
    }
    /// <summary>
    /// Первый зафиксированный срок истечения полученного JWT.
    /// </summary>
    public DateTimeOffset FirstExpiration => _expirations.First();

    /// <summary>
    /// Получает настоящие credentials с учётом режима отказа и при необходимости изменяет только подпись JWT.
    /// </summary>
    /// <remarks>
    /// Самостоятельный пятисекундный таймаут превращается в TimeoutException, чтобы штатный refresher повторил запрос. Внешняя отмена сохраняет семантику отмены worker.
    /// </remarks>
    /// <param name="cancellationToken">Токен отмены вызывающей стороны, включая завершение refresher.</param>
    /// <returns>Credentials, полученные у Keycloak, либо их копия с некорректной подписью.</returns>
    /// <exception cref="HttpRequestException">Включён управляемый отказ token endpoint.</exception>
    /// <exception cref="TimeoutException">Настоящий provider не ответил за пять секунд.</exception>
    /// <exception cref="OperationCanceledException">Запрос отменён вызывающей стороной.</exception>
    public async Task<Credentials> GetCredentialsAsync(CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _requests);
        Interlocked.Increment(ref _active);
        try
        {
            if (FailTokenRequests) throw new HttpRequestException("Simulated transient token endpoint failure.");
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(5));
            Credentials credentials;
            try
            {
                credentials = await inner.GetCredentialsAsync(timeout.Token);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && timeout.IsCancellationRequested)
            {
                // A stalled real endpoint is a retryable failure, not cancellation
                // of the native refresh worker's lifetime.
                throw new TimeoutException("Token endpoint did not respond within five seconds.");
            }
            // Read expiry for the timing oracle; RabbitMQ independently validates signature/audience.
            var payload = credentials.Password.Split('.')[1].Replace('-', '+').Replace('_', '/');
            payload = payload.PadRight((payload.Length + 3) / 4 * 4, '=');
            using var jwt = JsonDocument.Parse(Convert.FromBase64String(payload));
            _expirations.Enqueue(DateTimeOffset.FromUnixTimeSeconds(jwt.RootElement.GetProperty("exp").GetInt64()));
            if (!invalidToken) return credentials;
            var parts = credentials.Password.Split('.');
            var signature = parts[2].ToCharArray();
            signature[0] = signature[0] == 'A' ? 'B' : 'A';
            // Keep valid JSON, issuer, audience and expiry; only the signature is invalid.
            return new Credentials(Name, credentials.UserName,
                $"{parts[0]}.{parts[1]}.{new string(signature)}", credentials.ValidUntil);
        }
        catch
        {
            Interlocked.Increment(ref _failures);
            throw;
        }
        finally { Interlocked.Decrement(ref _active); }
    }

    /// <summary>
    /// Освобождает ресурсы исходного OAuth2 provider.
    /// </summary>
    public void Dispose() => inner.Dispose();
}

/// <summary>
/// Подсчитывает известные сообщения адаптера об успехах и ошибках обновления токена одной сессии.
/// </summary>
internal sealed class RefreshLog : ILoggerProvider
{
    private int _successes, _errors;
    /// <summary>
    /// Число завершённых UpdateSecretAsync, подтверждённых сообщением адаптера об успехе.
    /// </summary>
    public int Successes => Volatile.Read(ref _successes);
    /// <summary>
    /// Суммарное число ошибок получения credentials и передачи токена соединению.
    /// </summary>
    public int Errors => Volatile.Read(ref _errors);
    /// <inheritdoc/>
    public ILogger CreateLogger(string categoryName) => new CounterLogger(this);
    /// <inheritdoc/>
    public void Dispose() { }

    /// <summary>
    /// Распознаёт сообщения refresh-адаптера и атомарно обновляет счётчики владельца.
    /// </summary>
    /// <param name="owner">Общие счётчики текущей тестовой сессии.</param>
    private sealed class CounterLogger(RefreshLog owner) : ILogger
    {
        /// <inheritdoc/>
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        /// <inheritdoc/>
        public bool IsEnabled(LogLevel logLevel) => true;
        /// <inheritdoc/>
        public void Log<TState>(LogLevel level, EventId id, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            if (message == "RabbitMQ OAuth2 token refreshed successfully") Interlocked.Increment(ref owner._successes);
            if (message is "RabbitMQ OAuth2 credentials refresh failed" or "RabbitMQ OAuth2 connection token update failed")
                Interlocked.Increment(ref owner._errors);
        }
    }
}

/// <summary>
/// Передаёт уникальный идентификатор проверяемой доставки через RabbitMQ.
/// </summary>
/// <param name="Id">Идентификатор, связывающий публикацию с ожиданием обработчика.</param>
public sealed record ProbeMessage(Guid Id);

/// <summary>
/// Сопоставляет доставленные ID с ожидающими задачами в пределах одного host.
/// </summary>
public sealed class DeliveryProbe
{
    private readonly ConcurrentDictionary<Guid, TaskCompletionSource> _pending = new();
    /// <summary>
    /// Регистрирует ожидание обработки сообщения с указанным идентификатором.
    /// </summary>
    /// <param name="id">Уникальный ID ожидаемого сообщения.</param>
    /// <returns>Задача, которую завершит обработчик при получении данного ID.</returns>
    /// <exception cref="InvalidOperationException">Для этого ID уже существует незавершённое ожидание.</exception>
    public Task Expect(Guid id)
    {
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_pending.TryAdd(id, completion)) throw new InvalidOperationException("Duplicate message id");
        return completion.Task;
    }
    /// <summary>
    /// Завершает и удаляет ожидание полученного ID; неизвестные ID игнорируются.
    /// </summary>
    /// <param name="id">Идентификатор фактически обработанного сообщения.</param>
    public void Received(Guid id)
    {
        if (_pending.TryRemove(id, out var completion)) completion.TrySetResult();
    }
}

/// <summary>
/// Подтверждает получение сообщения через DeliveryProbe, внедрённый из текущего host.
/// </summary>
public sealed class ProbeMessageHandler
{
    /// <summary>
    /// Передаёт идентификатор полученного сообщения локальному ожиданию доставки.
    /// </summary>
    /// <param name="message">Сообщение, полученное Wolverine через RabbitMQ.</param>
    /// <param name="probe">Объект ожиданий, принадлежащий текущему host.</param>
    public static void Handle(ProbeMessage message, DeliveryProbe probe) => probe.Received(message.Id);
}
