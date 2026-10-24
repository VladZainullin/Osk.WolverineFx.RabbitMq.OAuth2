using HarmonyLib;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using TUnit.Mocks;
using Wolverine.RabbitMQ.Internal;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Tests;

/// <summary>
/// Проверяет Harmony-патчи и жизненный цикл обновления credentials на подставных AMQP-соединениях.
/// </summary>
/// <remarks>
/// Вызовы установки патчей меняют состояние методов во всём процессе, поэтому класс выполняется последовательно.
/// </remarks>
[NotInParallel]
public class PatchTests
{
    /// <summary>
    /// Проверяет идемпотентную установку единственного postfix создания соединения и пары prefix/postfix освобождения транспорта.
    /// </summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task InstallsBothPatchesExactlyOnce()
    {
        RabbitMqOAuth2Patches.Install();
        RabbitMqOAuth2Patches.Install();
        var create = AccessTools.DeclaredMethod(typeof(RabbitMqTransport), "CreateConnectionAsync");
        var dispose = AccessTools.DeclaredMethod(typeof(RabbitMqTransport), nameof(RabbitMqTransport.DisposeAsync));
        await Assert.That(Harmony.GetPatchInfo(create)!.Postfixes.Count(static p => p.owner == RabbitMqOAuth2Patches.HarmonyId)).IsEqualTo(1);
        await Assert.That(Harmony.GetPatchInfo(dispose)!.Prefixes.Count(static p => p.owner == RabbitMqOAuth2Patches.HarmonyId)).IsEqualTo(1);
        await Assert.That(Harmony.GetPatchInfo(dispose)!.Postfixes.Count(static p => p.owner == RabbitMqOAuth2Patches.HarmonyId)).IsEqualTo(1);
    }

    /// <summary>
    /// Проверяет передачу нового токена созданному соединению и освобождение refresh-регистрации через patched DisposeAsync транспорта.
    /// </summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task CreatedConnectionRefreshesAndPatchedTransportDisposalStopsWorker()
    {
        RabbitMqOAuth2Patches.Install();
        var transport = new RabbitMqTransport("rabbitmq");
        var connection = IConnection.Mock();
        await Assert.That(connection.Events.ConnectionShutdownAsync.SubscriberCount).IsEqualTo(0);
        var unsubscriptions = 0;
        connection.Events.ConnectionShutdownAsync.OnUnsubscribe(() => Interlocked.Increment(ref unsubscriptions));
        connection.IsOpen.Returns(true);
        var updated = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.UpdateSecretAsync(Any<string>(), Any<string>(), Any<CancellationToken>())
            .Callback((string token, string _, CancellationToken cancellationToken) => { updated.TrySetResult(token); })
            .ReturnsAsync(Task.CompletedTask);
        var provider = Provider("new-token", TimeSpan.FromMilliseconds(150));
        typeof(RabbitMqTransport).GetProperty(nameof(RabbitMqTransport.ConnectionFactory))!
            .SetValue(transport, new ConnectionFactory { CredentialsProvider = provider.Object });
        var original = Task.FromResult(connection.Object);
        RabbitMqTransportCreateConnectionAsyncPatch.Postfix(transport, ref original);
        var result = await original;
        await Assert.That(connection.Events.ConnectionShutdownAsync.SubscriberCount).IsEqualTo(1);
        await Assert.That(result).IsSameReferenceAs(connection.Object);
        await Assert.That(await updated.Task.WaitAsync(TimeSpan.FromSeconds(10))).IsEqualTo("new-token");
        await transport.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        provider.GetCredentialsAsync(Any<CancellationToken>()).WasCalled(Times.AtLeastOnce);
        await Assert.That(unsubscriptions).IsEqualTo(1);
        await Assert.That(connection.Events.ConnectionShutdownAsync.SubscriberCount).IsEqualTo(0);
    }

    /// <summary>
    /// Проверяет, что соединения с BasicCredentialsProvider сохраняют исходную задачу создания без OAuth2-обёртки.
    /// </summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task PasswordOnlyConnectionIsNotWrapped()
    {
        var transport = new RabbitMqTransport("rabbitmq");
        typeof(RabbitMqTransport).GetProperty(nameof(RabbitMqTransport.ConnectionFactory))!
            .SetValue(transport, new ConnectionFactory { CredentialsProvider = new BasicCredentialsProvider("basic", "user", "password") });
        var original = Task.FromResult(IConnection.Mock().Object);
        var result = original;
        RabbitMqTransportCreateConnectionAsyncPatch.Postfix(transport, ref result);
        await Assert.That((object)result).IsSameReferenceAs(original);
    }

    /// <summary>
    /// Проверяет сохранение исходной ошибки создания соединения без обращения к credentials provider.
    /// </summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task FailedConnectionDoesNotRequestCredentials()
    {
        var provider = Provider("token");
        var transport = new RabbitMqTransport("rabbitmq");
        typeof(RabbitMqTransport).GetProperty(nameof(RabbitMqTransport.ConnectionFactory))!
            .SetValue(transport, new ConnectionFactory { CredentialsProvider = provider.Object });
        var failure = new InvalidOperationException("connection failed");
        var actual = await Assert.That(async () =>
            await RabbitMqTransportCreateConnectionAsyncPatch.RegisterAsync(
                Task.FromException<IConnection>(failure), transport))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(actual).IsSameReferenceAs(failure);
        provider.GetCredentialsAsync(Any<CancellationToken>()).WasNeverCalled();
    }

    /// <summary>
    /// Проверяет регистрацию ошибки пустого токена, отсутствие вызова UpdateSecretAsync и завершение освобождения refresher.
    /// </summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task EmptyTokenIsNotSentAndDisposalStopsRefresher()
    {
        var connection = IConnection.Mock();
        connection.IsOpen.Returns(true);
        var requested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var logger = new ErrorLogger();
        var provider = ICredentialsProvider.Mock();
        provider.GetCredentialsAsync(Any<CancellationToken>())
            .Returns(() =>
            {
                requested.TrySetResult();
                return Task.FromResult(new Credentials("oauth", "user", "", TimeSpan.FromMinutes(1)));
            });
        await using var refresher = new RabbitMqCredentialsRefreshRegistration(connection.Object, provider.Object, logger);
        await requested.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await logger.Error.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await refresher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        connection.UpdateSecretAsync(Any<string>(), Any<string>(), Any<CancellationToken>()).WasNeverCalled();
    }

    /// <summary>
    /// Проверяет передачу соединению последовательно полученных токенов при повторных циклах штатного refresher.
    /// </summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task PeriodicRefreshesSendEachNewToken()
    {
        var connection = IConnection.Mock();
        connection.IsOpen.Returns(true);
        var firstUpdate = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondUpdate = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var updates = 0;
        connection.UpdateSecretAsync(Any<string>(), Any<string>(), Any<CancellationToken>())
            .Callback((string token, string reason, CancellationToken cancellationToken) =>
            {
                if (Interlocked.Increment(ref updates) == 1)
                {
                    firstUpdate.TrySetResult(token);
                }
                else
                {
                    secondUpdate.TrySetResult(token);
                }
            })
            .ReturnsAsync(Task.CompletedTask);
        var provider = ICredentialsProvider.Mock();
        var requests = 0;
        provider.GetCredentialsAsync(Any<CancellationToken>())
            .Returns(() => Task.FromResult(new Credentials(
                "oauth", "user", $"token-{Interlocked.Increment(ref requests)}", TimeSpan.FromMilliseconds(150))));

        await using var registration = new RabbitMqCredentialsRefreshRegistration(connection.Object, provider.Object, NullLogger.Instance);

        await Assert.That(await firstUpdate.Task.WaitAsync(TimeSpan.FromSeconds(10))).IsEqualTo("token-1");
        await Assert.That(await secondUpdate.Task.WaitAsync(TimeSpan.FromSeconds(10))).IsEqualTo("token-2");
    }

    /// <summary>
    /// Проверяет логирование ошибки credentials provider и успешную передачу токена при следующей попытке.
    /// </summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task ProviderFailureIsLoggedAndLaterRefreshContinues()
    {
        var connection = IConnection.Mock();
        connection.IsOpen.Returns(true);
        var recovered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.UpdateSecretAsync(Any<string>(), Any<string>(), Any<CancellationToken>())
            .Callback((string token, string _, CancellationToken cancellationToken) =>
            {
                if (token == "recovered-token")
                {
                    recovered.TrySetResult(token);
                }
            })
            .ReturnsAsync(Task.CompletedTask);
        var failure = new InvalidOperationException("provider unavailable");
        var provider = ICredentialsProvider.Mock();
        var requests = 0;
        provider.GetCredentialsAsync(Any<CancellationToken>())
            .Returns(() => Interlocked.Increment(ref requests) switch
            {
                1 => Task.FromResult(new Credentials("oauth", "user", "first-token", TimeSpan.FromMilliseconds(150))),
                2 => Task.FromException<Credentials>(failure),
                _ => Task.FromResult(new Credentials("oauth", "user", "recovered-token", TimeSpan.FromMilliseconds(150)))
            });
        var logger = new ErrorLogger();

        await using var registration = new RabbitMqCredentialsRefreshRegistration(connection.Object, provider.Object, logger);

        await Assert.That(await logger.Error.Task.WaitAsync(TimeSpan.FromSeconds(10))).IsSameReferenceAs(failure);
        await Assert.That(await recovered.Task.WaitAsync(TimeSpan.FromSeconds(10))).IsEqualTo("recovered-token");
        provider.GetCredentialsAsync(Any<CancellationToken>()).WasCalled(Times.AtLeast(3));
    }

    /// <summary>
    /// Проверяет логирование ошибки UpdateSecretAsync и успешное продолжение последующих обновлений.
    /// </summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task SecretUpdateFailureIsLoggedAndLaterRefreshContinues()
    {
        var connection = IConnection.Mock();
        connection.IsOpen.Returns(true);
        var failure = new InvalidOperationException("secret update unavailable");
        var recovered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var updates = 0;
        connection.UpdateSecretAsync(Any<string>(), Any<string>(), Any<CancellationToken>())
            .Returns(() =>
            {
                if (Interlocked.Increment(ref updates) == 1)
                {
                    return Task.FromException(failure);
                }

                recovered.TrySetResult();
                return Task.CompletedTask;
            });
        var provider = Provider("token", TimeSpan.FromMilliseconds(150));
        var logger = new ErrorLogger();

        await using var registration = new RabbitMqCredentialsRefreshRegistration(connection.Object, provider.Object, logger);

        await Assert.That(await logger.Error.Task.WaitAsync(TimeSpan.FromSeconds(10))).IsSameReferenceAs(failure);
        await recovered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        connection.UpdateSecretAsync(Any<string>(), Any<string>(), Any<CancellationToken>()).WasCalled(Times.AtLeast(2));
    }

    /// <summary>
    /// Проверяет освобождение поздно созданного соединения после остановки транспорта без запуска обновления credentials.
    /// </summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task ConnectionCompletedAfterTransportDisposalIsDisposedWithoutRefreshing()
    {
        RabbitMqOAuth2Patches.Install();
        var transport = new RabbitMqTransport("rabbitmq");
        var provider = Provider("token");
        var connection = IConnection.Mock();
        connection.IsOpen.Returns(true);
        connection.DisposeAsync()
            .ReturnsAsync(ValueTask.CompletedTask);
        var pending = new TaskCompletionSource<IConnection>(TaskCreationOptions.RunContinuationsAsynchronously);
        typeof(RabbitMqTransport).GetProperty(nameof(RabbitMqTransport.ConnectionFactory))!
            .SetValue(transport, new ConnectionFactory { CredentialsProvider = provider.Object });
        var registration = RabbitMqTransportCreateConnectionAsyncPatch.RegisterAsync(pending.Task, transport);

        await transport.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        pending.SetResult(connection.Object);

        await Assert.That(async () => await registration.WaitAsync(TimeSpan.FromSeconds(10)))
            .ThrowsExactly<ObjectDisposedException>();
        connection.DisposeAsync().WasCalled(Times.Once);
        provider.GetCredentialsAsync(Any<CancellationToken>()).WasNeverCalled();
    }

    /// <summary>
    /// Проверяет отмену активного запроса credentials и снятие подписки при закрытии соединения приложением.
    /// </summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task ApplicationShutdownCancelsOutstandingCredentialsRequest()
    {
        var connection = IConnection.Mock();
        await Assert.That(connection.Events.ConnectionShutdownAsync.SubscriberCount).IsEqualTo(0);
        var shutdown = new AwaitableShutdownConnection(connection.Object);
        var unsubscriptions = 0;
        connection.Events.ConnectionShutdownAsync.OnUnsubscribe(() => Interlocked.Increment(ref unsubscriptions));
        connection.IsOpen.Returns(true);
        var requested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var provider = ICredentialsProvider.Mock();
        provider.GetCredentialsAsync(Any<CancellationToken>())
            .Returns(async (CancellationToken cancellationToken) =>
            {
                using var callback = cancellationToken.Register(() => cancelled.TrySetResult());
                requested.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                return new Credentials("oauth", "user", "unused-token", TimeSpan.FromMinutes(1));
            });

        await using var registration = new RabbitMqCredentialsRefreshRegistration(shutdown, provider.Object, NullLogger.Instance);
        await Assert.That(connection.Events.ConnectionShutdownAsync.SubscriberCount).IsEqualTo(1);
        await requested.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await shutdown.RaiseAsync(new ShutdownEventArgs(ShutdownInitiator.Application, 200, "application stopping"));
        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await registration.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        connection.UpdateSecretAsync(Any<string>(), Any<string>(), Any<CancellationToken>()).WasNeverCalled();
        await Assert.That(unsubscriptions).IsEqualTo(1);
        await Assert.That(connection.Events.ConnectionShutdownAsync.SubscriberCount).IsEqualTo(0);
    }

    /// <summary>
    /// Проверяет обработку ожидаемой отмены штатного refresher во время задержки без ошибки DisposeAsync.
    /// </summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task CancellationDuringRefreshDelayDoesNotEscapeDisposal()
    {
        var connection = IConnection.Mock();
        connection.IsOpen.Returns(true);
        var updated = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.UpdateSecretAsync(Any<string>(), Any<string>(), Any<CancellationToken>())
            .Callback((string token, string reason, CancellationToken cancellationToken) =>
            {
                updated.TrySetResult();
            })
            .ReturnsAsync(Task.CompletedTask);
        var provider = Provider("token", TimeSpan.FromHours(1));

        await using var registration = new RabbitMqCredentialsRefreshRegistration(connection.Object, provider.Object, NullLogger.Instance);
        await updated.Task.WaitAsync(TimeSpan.FromSeconds(10));
        registration.Cancel();
        await registration.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        provider.GetCredentialsAsync(Any<CancellationToken>()).WasCalled(Times.Once);
    }

    /// <summary>
    /// Проверяет отсутствие UpdateSecretAsync у закрытого соединения, даже когда provider продолжает выдавать credentials.
    /// </summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task ClosedConnectionDoesNotReceiveSecretUpdates()
    {
        var connection = IConnection.Mock();
        connection.IsOpen.Returns(false);
        var nextRequest = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var provider = ICredentialsProvider.Mock();
        var requests = 0;
        provider.GetCredentialsAsync(Any<CancellationToken>())
            .Returns(() =>
            {
                if (Interlocked.Increment(ref requests) >= 2)
                {
                    nextRequest.TrySetResult();
                }

                return Task.FromResult(new Credentials("oauth", "user", "token", TimeSpan.FromMilliseconds(150)));
            });

        await using var registration = new RabbitMqCredentialsRefreshRegistration(connection.Object, provider.Object, NullLogger.Instance);
        await nextRequest.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await registration.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        connection.UpdateSecretAsync(Any<string>(), Any<string>(), Any<CancellationToken>()).WasNeverCalled();
    }

    /// <summary>
    /// Проверяет снятие shutdown-подписки и сохранение исходной ошибки при неудачной инициализации refresh-регистрации.
    /// </summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task FailedInitializationRemovesShutdownSubscription()
    {
        var connection = IConnection.Mock();
        var shutdown = new AwaitableShutdownConnection(connection.Object);
        var unsubscriptions = 0;
        connection.Events.ConnectionShutdownAsync.OnUnsubscribe(() => Interlocked.Increment(ref unsubscriptions));
        var failure = new InvalidOperationException("provider name unavailable");
        var provider = ICredentialsProvider.Mock();
        provider.Name.Throws(failure);
        provider.GetCredentialsAsync(Any<CancellationToken>())
            .Returns(static async (CancellationToken cancellationToken) =>
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                return new Credentials("oauth", "user", "unused-token", TimeSpan.FromMinutes(1));
            });

        var actual = await Assert.That(() => new RabbitMqCredentialsRefreshRegistration(
            shutdown, provider.Object, NullLogger.Instance)).ThrowsExactly<InvalidOperationException>();

        await Assert.That(actual).IsSameReferenceAs(failure);
        await Assert.That(connection.Events.ConnectionShutdownAsync.WasSubscribed).IsTrue();
        await Assert.That(unsubscriptions).IsEqualTo(1);
        await Assert.That(connection.Events.ConnectionShutdownAsync.SubscriberCount).IsEqualTo(0);
        await shutdown.RaiseAsync(new ShutdownEventArgs(ShutdownInitiator.Application, 200, "application stopping"));
        connection.UpdateSecretAsync(Any<string>(), Any<string>(), Any<CancellationToken>()).WasNeverCalled();
    }

    /// <summary>
    /// Проверяет завершение refresher и отмену активного запроса, когда освобождённое соединение запрещает отписку от события.
    /// </summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task AlreadyDisposedConnectionDoesNotPreventRefreshCleanup()
    {
        var connection = IConnection.Mock();
        await Assert.That(connection.Events.ConnectionShutdownAsync.SubscriberCount).IsEqualTo(0);
        var unsubscriptions = 0;
        connection.IsOpen.Returns(false);
        connection.Events.ConnectionShutdownAsync.OnUnsubscribe(() =>
        {
            Interlocked.Increment(ref unsubscriptions);
            throw new ObjectDisposedException("AutorecoveringConnection");
        });
        var requested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var provider = ICredentialsProvider.Mock();
        provider.GetCredentialsAsync(Any<CancellationToken>())
            .Returns(async (CancellationToken cancellationToken) =>
            {
                using var callback = cancellationToken.Register(() => cancelled.TrySetResult());
                requested.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                return new Credentials("oauth", "user", "unused-token", TimeSpan.FromMinutes(1));
            });

        await using var registration = new RabbitMqCredentialsRefreshRegistration(connection.Object, provider.Object, NullLogger.Instance);
        await Assert.That(connection.Events.ConnectionShutdownAsync.SubscriberCount).IsEqualTo(1);
        await requested.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await registration.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(unsubscriptions).IsEqualTo(1);
        await Assert.That(connection.Events.ConnectionShutdownAsync.SubscriberCount).IsEqualTo(0);
    }

    /// <summary>
    /// Проверяет, что параллельные вызовы DisposeAsync ожидают незавершённый UpdateSecretAsync и снимают shutdown-подписку ровно один раз.
    /// </summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task ConcurrentDisposalsWaitForInFlightUpdateAndUnsubscribeOnce()
    {
        var connection = IConnection.Mock();
        await Assert.That(connection.Events.ConnectionShutdownAsync.SubscriberCount).IsEqualTo(0);
        var unsubscriptions = 0;
        connection.Events.ConnectionShutdownAsync.OnUnsubscribe(() => Interlocked.Increment(ref unsubscriptions));
        connection.IsOpen.Returns(true);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // Для Task без результата версия 1.74.0 поддерживает типизированный Callback,
        // но фабрика ReturnsAsync не принимает аргументы исходного метода.
        var pendingUpdate = Task.CompletedTask;
        connection.UpdateSecretAsync(Any<string>(), Any<string>(), Any<CancellationToken>())
            .Callback((string _, string _, CancellationToken cancellationToken) => pendingUpdate = UpdateAsync(cancellationToken))
            .ReturnsAsync(() => pendingUpdate);

        async Task UpdateAsync(CancellationToken cancellationToken)
        {
            await using var callback = cancellationToken.Register(() => cancelled.TrySetResult());
            entered.TrySetResult();
            await release.Task;
            cancellationToken.ThrowIfCancellationRequested();
        }

        var provider = Provider("token");
        await using var registration = new RabbitMqCredentialsRefreshRegistration(connection.Object, provider.Object, NullLogger.Instance);
        await Assert.That(connection.Events.ConnectionShutdownAsync.SubscriberCount).IsEqualTo(1);

        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var disposals = Enumerable.Range(0, 8).Select(_ => registration.DisposeAsync().AsTask()).ToArray();
            await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.That(disposals.All(static task => !task.IsCompleted)).IsTrue();
            release.TrySetResult();
            await Task.WhenAll(disposals).WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.That(unsubscriptions).IsEqualTo(1);
            await Assert.That(connection.Events.ConnectionShutdownAsync.SubscriberCount).IsEqualTo(0);
        }
        finally
        {
            release.TrySetResult();
        }
    }

    /// <summary>
    /// Создаёт подставной provider с заданными токеном и относительным сроком действия credentials.
    /// </summary>
    /// <param name="token">Значение тестового токена.</param>
    /// <param name="validUntil">Относительный срок действия; при отсутствии значения используется одна минута.</param>
    /// <returns>Mock, возвращающий заданные credentials при каждом запросе.</returns>
    private static Mock<ICredentialsProvider> Provider(string token, TimeSpan? validUntil = null)
    {
        var provider = ICredentialsProvider.Mock();
        provider.GetCredentialsAsync(Any<CancellationToken>())
            .Returns(new Credentials("oauth", "user", token, validUntil ?? TimeSpan.FromMinutes(1)));
        return provider;
    }

    /// <summary>
    /// Фиксирует первое сообщение уровня Error для ожидания диагностического события без задержек.
    /// </summary>
    private sealed class ErrorLogger : ILogger
    {
        /// <summary>
        /// Источник завершения с исключением первого сообщения уровня Error либо null, если исключения нет.
        /// </summary>
        public TaskCompletionSource<Exception?> Error { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        /// <inheritdoc/>
        public bool IsEnabled(LogLevel logLevel) => true;

        /// <inheritdoc/>
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Error)
            {
                Error.TrySetResult(exception);
            }
        }
    }
}
