using HarmonyLib;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Wolverine.RabbitMQ.Internal;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Patch.Tests;

// Harmony changes process-wide method state, so these tests run sequentially.
[NotInParallel]
public class PatchTests
{
    [Test]
    public async Task InstallsBothPatchesExactlyOnce()
    {
        RabbitMqOAuth2Patches.Install();
        RabbitMqOAuth2Patches.Install();
        var create = AccessTools.DeclaredMethod(typeof(RabbitMqTransport), "CreateConnectionAsync");
        var dispose = AccessTools.DeclaredMethod(typeof(RabbitMqTransport), nameof(RabbitMqTransport.DisposeAsync));
        await Assert.That(Harmony.GetPatchInfo(create)!.Postfixes.Count(p => p.owner == RabbitMqOAuth2Patches.HarmonyId)).IsEqualTo(1);
        await Assert.That(Harmony.GetPatchInfo(dispose)!.Prefixes.Count(p => p.owner == RabbitMqOAuth2Patches.HarmonyId)).IsEqualTo(1);
        await Assert.That(Harmony.GetPatchInfo(dispose)!.Postfixes.Count(p => p.owner == RabbitMqOAuth2Patches.HarmonyId)).IsEqualTo(1);
    }

    [Test]
    public async Task CreatedConnectionRefreshesAndPatchedTransportDisposalStopsWorker()
    {
        RabbitMqOAuth2Patches.Install();
        var transport = new RabbitMqTransport("rabbitmq");
        var connection = new Mock<IConnection>();
        connection.SetupGet(x => x.IsOpen).Returns(true);
        var updated = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.Setup(x => x.UpdateSecretAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns((string token, string reason, CancellationToken ct) => { updated.TrySetResult(token); return Task.CompletedTask; });
        var provider = Provider("new-token", TimeSpan.FromMilliseconds(150));
        typeof(RabbitMqTransport).GetProperty(nameof(RabbitMqTransport.ConnectionFactory))!
            .SetValue(transport, new ConnectionFactory { CredentialsProvider = provider.Object });
        var original = Task.FromResult(connection.Object);
        RabbitMqTransportCreateConnectionAsyncPatch.Postfix(transport, ref original);
        var result = await original;
        await Assert.That(result).IsSameReferenceAs(connection.Object);
        await Assert.That(await updated.Task.WaitAsync(TimeSpan.FromSeconds(10))).IsEqualTo("new-token");
        await transport.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        provider.Verify(x => x.GetCredentialsAsync(It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        connection.VerifyRemove(x => x.ConnectionShutdownAsync -= It.IsAny<RabbitMQ.Client.Events.AsyncEventHandler<ShutdownEventArgs>>(), Times.Once);
    }

    [Test]
    public async Task PasswordOnlyConnectionIsNotWrapped()
    {
        var transport = new RabbitMqTransport("rabbitmq");
        typeof(RabbitMqTransport).GetProperty(nameof(RabbitMqTransport.ConnectionFactory))!
            .SetValue(transport, new ConnectionFactory { CredentialsProvider = new BasicCredentialsProvider("basic", "user", "password") });
        var original = Task.FromResult(new Mock<IConnection>().Object);
        var result = original;
        RabbitMqTransportCreateConnectionAsyncPatch.Postfix(transport, ref result);
        await Assert.That((object)result).IsSameReferenceAs(original);
    }

    [Test]
    public async Task FailedConnectionDoesNotRequestCredentials()
    {
        var provider = Provider("token");
        var failure = new InvalidOperationException("connection failed");
        var actual = await Assert.That(async () =>
            await RabbitMqTransportCreateConnectionAsyncPatch.RegisterAsync(
                Task.FromException<IConnection>(failure), new RabbitMqTransport("rabbitmq"), provider.Object))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(actual).IsSameReferenceAs(failure);
        provider.Verify(x => x.GetCredentialsAsync(It.IsAny<CancellationToken>()), Times.Never);
    }

    [Test]
    public async Task EmptyTokenIsNotSentAndDisposalStopsRefresher()
    {
        var connection = new Mock<IConnection>();
        connection.SetupGet(x => x.IsOpen).Returns(true);
        var requested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var logger = new ErrorLogger();
        var provider = new Mock<ICredentialsProvider>();
        provider.Setup(x => x.GetCredentialsAsync(It.IsAny<CancellationToken>())).Returns(() =>
        {
            requested.TrySetResult();
            return Task.FromResult(new Credentials("oauth", "user", "", TimeSpan.FromMinutes(1)));
        });
        await using var refresher = new RabbitMqCredentialsRefreshRegistration(connection.Object, provider.Object, logger);
        await requested.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await logger.Error.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await refresher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        connection.Verify(x => x.UpdateSecretAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Test]
    public async Task PeriodicRefreshesSendEachNewToken()
    {
        var connection = new Mock<IConnection>();
        connection.SetupGet(x => x.IsOpen).Returns(true);
        var firstUpdate = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondUpdate = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var updates = 0;
        connection.Setup(x => x.UpdateSecretAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns((string token, string reason, CancellationToken ct) =>
            {
                if (Interlocked.Increment(ref updates) == 1)
                {
                    firstUpdate.TrySetResult(token);
                }
                else
                {
                    secondUpdate.TrySetResult(token);
                }

                return Task.CompletedTask;
            });
        var provider = new Mock<ICredentialsProvider>();
        var requests = 0;
        provider.Setup(x => x.GetCredentialsAsync(It.IsAny<CancellationToken>()))
            .Returns(() => Task.FromResult(new Credentials(
                "oauth", "user", $"token-{Interlocked.Increment(ref requests)}", TimeSpan.FromMilliseconds(150))));

        await using var registration = new RabbitMqCredentialsRefreshRegistration(connection.Object, provider.Object, NullLogger.Instance);

        await Assert.That(await firstUpdate.Task.WaitAsync(TimeSpan.FromSeconds(10))).IsEqualTo("token-1");
        await Assert.That(await secondUpdate.Task.WaitAsync(TimeSpan.FromSeconds(10))).IsEqualTo("token-2");
    }

    [Test]
    public async Task ProviderFailureIsLoggedAndLaterRefreshContinues()
    {
        var connection = new Mock<IConnection>();
        connection.SetupGet(x => x.IsOpen).Returns(true);
        var recovered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.Setup(x => x.UpdateSecretAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns((string token, string reason, CancellationToken ct) =>
            {
                if (token == "recovered-token")
                {
                    recovered.TrySetResult(token);
                }

                return Task.CompletedTask;
            });
        var failure = new InvalidOperationException("provider unavailable");
        var provider = new Mock<ICredentialsProvider>();
        var requests = 0;
        provider.Setup(x => x.GetCredentialsAsync(It.IsAny<CancellationToken>()))
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
        provider.Verify(x => x.GetCredentialsAsync(It.IsAny<CancellationToken>()), Times.AtLeast(3));
    }

    [Test]
    public async Task SecretUpdateFailureIsLoggedAndLaterRefreshContinues()
    {
        var connection = new Mock<IConnection>();
        connection.SetupGet(x => x.IsOpen).Returns(true);
        var failure = new InvalidOperationException("secret update unavailable");
        var recovered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var updates = 0;
        connection.Setup(x => x.UpdateSecretAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns((string token, string reason, CancellationToken ct) =>
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
        connection.Verify(x => x.UpdateSecretAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.AtLeast(2));
    }

    [Test]
    public async Task ConnectionCompletedAfterTransportDisposalIsDisposedWithoutRefreshing()
    {
        RabbitMqOAuth2Patches.Install();
        var transport = new RabbitMqTransport("rabbitmq");
        var provider = Provider("token");
        var connection = new Mock<IConnection>();
        connection.SetupGet(x => x.IsOpen).Returns(true);
        connection.Setup(x => x.DisposeAsync()).Returns(ValueTask.CompletedTask);
        var pending = new TaskCompletionSource<IConnection>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registration = RabbitMqTransportCreateConnectionAsyncPatch.RegisterAsync(pending.Task, transport, provider.Object);

        await transport.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        pending.SetResult(connection.Object);

        await Assert.That(async () => await registration.WaitAsync(TimeSpan.FromSeconds(10)))
            .ThrowsExactly<ObjectDisposedException>();
        connection.Verify(x => x.DisposeAsync(), Times.Once);
        provider.Verify(x => x.GetCredentialsAsync(It.IsAny<CancellationToken>()), Times.Never);
    }

    [Test]
    public async Task ApplicationShutdownCancelsOutstandingCredentialsRequest()
    {
        var connection = new Mock<IConnection>();
        connection.SetupGet(x => x.IsOpen).Returns(true);
        var requested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var provider = new Mock<ICredentialsProvider>();
        provider.Setup(x => x.GetCredentialsAsync(It.IsAny<CancellationToken>()))
            .Returns(async (CancellationToken ct) =>
            {
                using var callback = ct.Register(() => cancelled.TrySetResult());
                requested.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                return new Credentials("oauth", "user", "unused-token", TimeSpan.FromMinutes(1));
            });

        await using var registration = new RabbitMqCredentialsRefreshRegistration(connection.Object, provider.Object, NullLogger.Instance);
        await requested.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await connection.RaiseAsync(x => x.ConnectionShutdownAsync += null,
            connection.Object, new ShutdownEventArgs(ShutdownInitiator.Application, 200, "application stopping"));
        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await registration.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        connection.Verify(x => x.UpdateSecretAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        connection.VerifyRemove(x => x.ConnectionShutdownAsync -= It.IsAny<RabbitMQ.Client.Events.AsyncEventHandler<ShutdownEventArgs>>(), Times.Once);
    }

    [Test]
    public async Task CancellationDuringRefreshDelayDoesNotEscapeDisposal()
    {
        var connection = new Mock<IConnection>();
        connection.SetupGet(x => x.IsOpen).Returns(true);
        var updated = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.Setup(x => x.UpdateSecretAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns((string token, string reason, CancellationToken ct) =>
            {
                updated.TrySetResult();
                return Task.CompletedTask;
            });
        var provider = Provider("token", TimeSpan.FromHours(1));

        await using var registration = new RabbitMqCredentialsRefreshRegistration(connection.Object, provider.Object, NullLogger.Instance);
        await updated.Task.WaitAsync(TimeSpan.FromSeconds(10));
        registration.Cancel();
        await registration.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        provider.Verify(x => x.GetCredentialsAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Test]
    public async Task ClosedConnectionDoesNotReceiveSecretUpdates()
    {
        var connection = new Mock<IConnection>();
        connection.SetupGet(x => x.IsOpen).Returns(false);
        var nextRequest = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var provider = new Mock<ICredentialsProvider>();
        var requests = 0;
        provider.Setup(x => x.GetCredentialsAsync(It.IsAny<CancellationToken>()))
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

        connection.Verify(x => x.UpdateSecretAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Test]
    public async Task FailedInitializationRemovesShutdownSubscription()
    {
        var connection = new Mock<IConnection>();
        var failure = new InvalidOperationException("provider name unavailable");
        var provider = new Mock<ICredentialsProvider>();
        provider.SetupGet(x => x.Name).Throws(failure);
        provider.Setup(x => x.GetCredentialsAsync(It.IsAny<CancellationToken>()))
            .Returns(async (CancellationToken ct) =>
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                return new Credentials("oauth", "user", "unused-token", TimeSpan.FromMinutes(1));
            });

        var actual = await Assert.That(() => new RabbitMqCredentialsRefreshRegistration(
            connection.Object, provider.Object, NullLogger.Instance)).ThrowsExactly<InvalidOperationException>();

        await Assert.That(actual).IsSameReferenceAs(failure);
        connection.VerifyRemove(x => x.ConnectionShutdownAsync -= It.IsAny<AsyncEventHandler<ShutdownEventArgs>>(), Times.Once);
        // Moq returns null when the event has no subscribers after cleanup.
        await (connection.RaiseAsync(x => x.ConnectionShutdownAsync += null,
            connection.Object, new ShutdownEventArgs(ShutdownInitiator.Application, 200, "application stopping"))
            ?? Task.CompletedTask);
        connection.Verify(x => x.UpdateSecretAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Test]
    public async Task ConcurrentDisposalsWaitForInFlightUpdateAndUnsubscribeOnce()
    {
        var connection = new Mock<IConnection>();
        connection.SetupGet(x => x.IsOpen).Returns(true);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.Setup(x => x.UpdateSecretAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(async (string token, string reason, CancellationToken ct) =>
            {
                using var callback = ct.Register(() => cancelled.TrySetResult());
                entered.TrySetResult();
                await release.Task;
                ct.ThrowIfCancellationRequested();
            });
        var provider = Provider("token");
        await using var registration = new RabbitMqCredentialsRefreshRegistration(connection.Object, provider.Object, NullLogger.Instance);

        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var disposals = Enumerable.Range(0, 8).Select(_ => registration.DisposeAsync().AsTask()).ToArray();
            await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.That(disposals.All(task => !task.IsCompleted)).IsTrue();
            release.TrySetResult();
            await Task.WhenAll(disposals).WaitAsync(TimeSpan.FromSeconds(10));
            connection.VerifyRemove(x => x.ConnectionShutdownAsync -= It.IsAny<RabbitMQ.Client.Events.AsyncEventHandler<ShutdownEventArgs>>(), Times.Once);
        }
        finally
        {
            release.TrySetResult();
        }
    }

    private static Mock<ICredentialsProvider> Provider(string token, TimeSpan? validUntil = null)
    {
        var provider = new Mock<ICredentialsProvider>();
        provider.Setup(x => x.GetCredentialsAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new Credentials("oauth", "user", token, validUntil ?? TimeSpan.FromMinutes(1)));
        return provider;
    }

    private sealed class ErrorLogger : ILogger
    {
        public TaskCompletionSource<Exception?> Error { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Error)
            {
                Error.TrySetResult(exception);
            }
        }
    }
}
