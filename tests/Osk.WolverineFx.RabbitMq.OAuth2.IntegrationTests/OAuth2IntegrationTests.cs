using System.Diagnostics;
using RabbitMQ.Client.Exceptions;
using TUnit.Core.Interfaces;

namespace Osk.WolverineFx.RabbitMq.OAuth2.IntegrationTests;

/// <summary>
/// Проверяет аутентификацию, обновление JWT и завершение работы Wolverine на настоящих Keycloak и RabbitMQ.
/// </summary>
/// <remarks>
/// Контейнеры и сеть общие для класса; host, очереди, providers и ожидания сообщений принадлежат отдельным тестам. Обычные сценарии выполняются с лимитом четыре, а недоступность общего Keycloak проверяется эксклюзивно.
/// </remarks>
[ParallelLimiter<OAuth2ParallelLimit>]
[Category("Integration")]
[Timeout(180_000)]
public sealed class OAuth2IntegrationTests
{
    private static OAuth2Environment? _environment;
    private static int _activeTests, _maximumParallelTests;
    private readonly Stopwatch _duration = new();
    private static OAuth2Environment Environment => _environment ?? throw new InvalidOperationException("Containers have not started");

    /// <summary>
    /// Создаёт общую инфраструктуру класса и ожидает готовности обоих сервисов.
    /// </summary>
    /// <param name="ct">Токен отмены, предоставленный TUnit для hook.</param>
    /// <returns>Задача запуска контейнеров и readiness-проверок.</returns>
    [Before(HookType.Class)]
    [Timeout(300_000)]
    public static async Task StartContainers(CancellationToken ct)
    {
        _environment = new OAuth2Environment();
        await _environment.StartAsync(ct);
    }

    /// <summary>
    /// Освобождает общую инфраструктуру после всех тестов и выводит достигнутый параллелизм.
    /// </summary>
    /// <param name="ct">Токен отмены, предоставленный TUnit для hook.</param>
    /// <returns>Задача освобождения контейнеров, сети, HTTP-клиента и сертификатов.</returns>
    [After(HookType.Class)]
    public static async Task StopContainers(CancellationToken ct)
    {
        if (_environment is not null) await _environment.DisposeAsync();
        Console.WriteLine($"[timing] maximum parallel tests: {_maximumParallelTests}");
    }

    /// <summary>
    /// Начинает замер теста и потокобезопасно обновляет число одновременно выполняющихся сценариев.
    /// </summary>
    /// <param name="context">Контекст TUnit с именем текущего теста.</param>
    /// <param name="ct">Токен отмены, предоставленный TUnit для hook.</param>
    [Before(HookType.Test)]
    public void RecordTestStart(TestContext context, CancellationToken ct)
    {
        _duration.Start();
        var active = Interlocked.Increment(ref _activeTests);
        int previous;
        do
        {
            previous = Volatile.Read(ref _maximumParallelTests);
            if (active <= previous) break;
        } while (Interlocked.CompareExchange(ref _maximumParallelTests, active, previous) != previous);
        Console.WriteLine($"[timing] test {context.Metadata.TestName} start: {DateTimeOffset.UtcNow:O}; active: {active}");
    }

    /// <summary>
    /// Завершает замер теста и уменьшает счётчик активных сценариев.
    /// </summary>
    /// <param name="context">Контекст TUnit с именем завершённого теста.</param>
    /// <param name="ct">Токен отмены, предоставленный TUnit для hook.</param>
    [After(HookType.Test)]
    public void RecordTestEnd(TestContext context, CancellationToken ct)
    {
        _duration.Stop();
        Interlocked.Decrement(ref _activeTests);
        Console.WriteLine($"[timing] test {context.Metadata.TestName}: {_duration.Elapsed.TotalMilliseconds:F0} ms");
    }

    /// <summary>
    /// Проверяет открытие двух AMQP-соединений с JWT Keycloak и доставку сообщения через RabbitMQ.
    /// </summary>
    /// <param name="ct">Токен отмены, предоставленный TUnit для выполнения проверки.</param>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task ConnectsWithValidToken(CancellationToken ct)
    {
        await using var session = await WolverineSession.CreateAsync(Environment, ct: ct);
        await session.StartAsync(ct);
        await session.SendAndReceiveAsync(ct);
        await Assert.That(session.Credentials.Requests >= 2).IsTrue();
        await WaitForConnectionsAsync(session, ct);
        await Assert.That((await Environment.ConnectionIdsAsync(session.ConnectionName)).Length).IsEqualTo(2);
    }

    /// <summary>
    /// Проверяет отказ аутентификации при изменённой подписи JWT и отсутствие оставшихся соединений.
    /// </summary>
    /// <remarks>
    /// Payload, audience и срок действия токена сохраняются; изменяется только подпись. Успешных обновлений credentials быть не должно.
    /// </remarks>
    /// <param name="ct">Токен отмены, предоставленный TUnit для выполнения проверки.</param>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task RejectsInvalidToken(CancellationToken ct)
    {
        await using var session = await WolverineSession.CreateAsync(Environment, invalidToken: true, ct: ct);
        var failure = await Assert.That(async () => await session.StartAsync(ct)).Throws<Exception>();
        await Assert.That(ExceptionChain(failure!).Any(e => e is AuthenticationFailureException or PossibleAuthenticationFailureException)).IsTrue();
        await OAuth2Environment.WaitUntilAsync(async () => (await Environment.ConnectionIdsAsync(session.ConnectionName)).Length == 0,
            TimeSpan.FromSeconds(10), ct);
        await Assert.That(session.Log.Successes).IsEqualTo(0);
    }

    /// <summary>
    /// Проверяет успешное обновление credentials и доставку трёх сообщений после истечения первоначального JWT.
    /// </summary>
    /// <remarks>
    /// Для каждого сообщения ожидается собственный ID в обработчике; статистика брокера должна подтвердить доставку всей группы.
    /// </remarks>
    /// <param name="ct">Токен отмены, предоставленный TUnit для выполнения проверки.</param>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task RefreshesExpiredToken(CancellationToken ct)
    {
        await using var session = await WolverineSession.CreateAsync(Environment, ct: ct);
        await session.StartAsync(ct);
        await session.SendAndReceiveAsync(ct);
        var initialRefreshes = session.Log.Successes;
        await WaitPastInitialExpiryAsync(session, ct);
        await OAuth2Environment.WaitUntilAsync(() => Task.FromResult(session.Log.Successes > initialRefreshes), TimeSpan.FromSeconds(15), ct);
        // Distinct messages after the original JWT has expired must traverse the real broker.
        await session.SendAndReceiveAsync(ct, messageCount: 3);
    }

    /// <summary>
    /// Проверяет, что обновление JWT и доставка после исходного срока действия сохраняют оба AMQP-соединения.
    /// </summary>
    /// <remarks>
    /// Сравниваются имена и время установки соединений в Management API: повторное подключение изменит их идентификаторы.
    /// </remarks>
    /// <param name="ct">Токен отмены, предоставленный TUnit для выполнения проверки.</param>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task PreservesConnectionAfterRefresh(CancellationToken ct)
    {
        await using var session = await WolverineSession.CreateAsync(Environment, ct: ct);
        await session.StartAsync(ct);
        await WaitForConnectionsAsync(session, ct);
        var before = await Environment.ConnectionIdsAsync(session.ConnectionName);
        await Assert.That(before.Length).IsEqualTo(2);
        var refreshes = session.Log.Successes;
        await WaitPastInitialExpiryAsync(session, ct);
        await OAuth2Environment.WaitUntilAsync(() => Task.FromResult(session.Log.Successes > refreshes), TimeSpan.FromSeconds(15), ct);
        await session.SendAndReceiveAsync(ct);
        var after = await Environment.ConnectionIdsAsync(session.ConnectionName);
        // Broker-side name + connected_at distinguishes recovery/reconnect from update-secret.
        await Assert.That(after.SequenceEqual(before)).IsTrue();
    }

    /// <summary>
    /// Проверяет восстановление обновления JWT и доставки после управляемого отказа credentials provider.
    /// </summary>
    /// <remarks>
    /// Ошибка включается только у provider текущей сессии, до обращения к Keycloak. Реальная недоступность сервиса проверяется отдельно в <see cref="RecoversAfterKeycloakOutage"/>.
    /// </remarks>
    /// <param name="ct">Токен отмены, предоставленный TUnit для выполнения проверки.</param>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task RecoversAfterTokenEndpointFailure(CancellationToken ct)
    {
        await using var session = await WolverineSession.CreateAsync(Environment, ct: ct);
        await session.StartAsync(ct);
        await session.SendAndReceiveAsync(ct);
        var failures = session.Credentials.Failures;
        var errors = session.Log.Errors;
        var refreshes = session.Log.Successes;
        session.Credentials.FailTokenRequests = true;
        try
        {
            await OAuth2Environment.WaitUntilAsync(() => Task.FromResult(session.Credentials.Failures > failures && session.Log.Errors > errors),
                TimeSpan.FromSeconds(25), ct);
            refreshes = session.Log.Successes;
        }
        finally
        {
            session.Credentials.FailTokenRequests = false;
        }

        await OAuth2Environment.WaitUntilAsync(() => Task.FromResult(session.Log.Successes >= refreshes + 2), TimeSpan.FromSeconds(60), ct);
        await session.SendAndReceiveAsync(ct);
    }

    /// <summary>
    /// Проверяет восстановление обновления JWT и доставки после фактической недоступности token endpoint Keycloak.
    /// </summary>
    /// <remarks>
    /// Docker Pause/Unpause сохраняет назначенный порт и делает общий endpoint недоступным. Сценарий категории Slow выполняется эксклюзивно и перед публикацией ожидает обновлений токена, соединений и consumer своей очереди; перезапуск JVM здесь не проверяется.
    /// </remarks>
    /// <param name="ct">Токен отмены, предоставленный TUnit для выполнения проверки.</param>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    [Category("Slow")]
    [NotInParallel]
    public async Task RecoversAfterKeycloakOutage(CancellationToken ct)
    {
        await using var session = await WolverineSession.CreateAsync(Environment, ct: ct);
        await session.StartAsync(ct);
        await session.SendAndReceiveAsync(ct);
        var failures = session.Credentials.Failures;
        var errors = session.Log.Errors;
        var refreshes = session.Log.Successes;
        // Pause preserves the dynamic port and makes the real token endpoint
        // unavailable. The provider bounds each HTTP request to five seconds.
        await Environment.Keycloak.PauseAsync(ct);
        try
        {
            await OAuth2Environment.WaitUntilAsync(
                () => Task.FromResult(session.Credentials.Failures > failures && session.Log.Errors > errors),
                TimeSpan.FromSeconds(25), ct);
            refreshes = session.Log.Successes;
        }
        finally
        {
            await Environment.Keycloak.UnpauseAsync();
            await Environment.WaitForKeycloakAsync(ct);
        }

        // A single successful callback can belong only to the sending connection.
        // Wait for two completed updates and broker-side listener readiness before publishing.
        await OAuth2Environment.WaitUntilAsync(() => Task.FromResult(session.Log.Successes >= refreshes + 2), TimeSpan.FromSeconds(60), ct);
        await WaitForConnectionsAsync(session, ct);
        await OAuth2Environment.WaitUntilAsync(async () => await Environment.ConsumerCountAsync(session.QueueName, ct) > 0,
            TimeSpan.FromSeconds(15), ct);
        await session.SendAndReceiveAsync(ct);
    }

    /// <summary>
    /// Проверяет завершение задач refresher, отсутствие активных запросов credentials и закрытие AMQP-соединений после Dispose.
    /// </summary>
    /// <remarks>
    /// Worker ожидает получение credentials и callback с UpdateSecretAsync. Его завершение исключает незавершённые обновления и будущие попытки, поэтому ожидание двух TTL не требуется.
    /// </remarks>
    /// <param name="ct">Токен отмены, предоставленный TUnit для выполнения проверки.</param>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task StopsRefreshingAfterDispose(CancellationToken ct)
    {
        await using var session = await WolverineSession.CreateAsync(Environment, ct: ct);
        await session.StartAsync(ct);
        await session.SendAndReceiveAsync(ct);
        var refreshers = session.CaptureRefreshDiagnostics();
        await Assert.That(refreshers.Length).IsEqualTo(2);
        await session.DisposeAsync();
        // Each worker awaits credentials acquisition and its UpdateSecretAsync callback.
        // Completion therefore proves there is no pending refresh operation or future timer.
        await Assert.That(refreshers.All(refresher => refresher.Worker.IsCompleted)).IsTrue();
        await Assert.That(refreshers.All(refresher => !refresher.Connection.IsOpen)).IsTrue();
        await Assert.That(session.Credentials.ActiveRequests).IsEqualTo(0);
        var requests = session.Credentials.Requests;
        var refreshes = session.Log.Successes;
        await OAuth2Environment.WaitUntilAsync(async () => (await Environment.ConnectionIdsAsync(session.ConnectionName)).Length == 0,
            TimeSpan.FromSeconds(10), ct);
        await Assert.That(session.Credentials.Requests).IsEqualTo(requests);
        await Assert.That(session.Log.Successes).IsEqualTo(refreshes);
        await Assert.That(session.Credentials.ActiveRequests).IsEqualTo(0);
    }

    /// <summary>
    /// Ожидает пересечения срока действия первого JWT по целым секундам.
    /// </summary>
    /// <remarks>
    /// JWT и часы брокера имеют секундную точность; дополнительная произвольная задержка не используется.
    /// </remarks>
    /// <param name="session">Сессия с зафиксированным сроком действия первого токена.</param>
    /// <param name="ct">Токен отмены, предоставленный TUnit для выполнения проверки.</param>
    /// <returns>Задача ожидания фактического истечения исходного JWT.</returns>
    private static async Task WaitPastInitialExpiryAsync(WolverineSession session, CancellationToken ct)
    {
        // JWT exp and the broker's clock have whole-second precision. Cross that
        // boundary before sending; waiting for real expiry is part of this assertion.
        await OAuth2Environment.WaitUntilAsync(
            () => Task.FromResult(DateTimeOffset.UtcNow.ToUnixTimeSeconds() > session.Credentials.FirstExpiration.ToUnixTimeSeconds()),
            TimeSpan.FromSeconds(OAuth2Environment.TokenLifetimeSeconds + 5), ct);
        await Assert.That(DateTimeOffset.UtcNow > session.Credentials.FirstExpiration).IsTrue();
    }

    /// <summary>
    /// Ожидает регистрации в Management API двух соединений указанной сессии.
    /// </summary>
    /// <param name="session">Сессия с уникальным именем AMQP-соединений.</param>
    /// <param name="ct">Токен отмены, предоставленный TUnit для выполнения проверки.</param>
    /// <returns>Задача ожидания готовности обоих соединений.</returns>
    private static Task WaitForConnectionsAsync(WolverineSession session, CancellationToken ct) =>
        OAuth2Environment.WaitUntilAsync(async () => (await Environment.ConnectionIdsAsync(session.ConnectionName)).Length == 2,
            TimeSpan.FromSeconds(15), ct);

    /// <summary>
    /// Перечисляет исходное исключение и вложенные причины, разворачивая AggregateException.
    /// </summary>
    /// <param name="exception">Исключение, причины которого требуется проверить.</param>
    /// <returns>Последовательность всех исключений в цепочке причин.</returns>
    private static IEnumerable<Exception> ExceptionChain(Exception exception)
    {
        yield return exception;
        if (exception is AggregateException aggregate)
        {
            foreach (var inner in aggregate.InnerExceptions.SelectMany(ExceptionChain)) yield return inner;
        }
        else if (exception.InnerException is { } inner)
        {
            foreach (var nested in ExceptionChain(inner)) yield return nested;
        }
    }
}

/// <summary>
/// Задаёт общий лимит одновременного выполнения обычных OAuth2-сценариев.
/// </summary>
public sealed class OAuth2ParallelLimit : IParallelLimit
{
    /// <summary>
    /// Максимальное число одновременно выполняемых тестов: четыре.
    /// </summary>
    public int Limit => 4;
}
