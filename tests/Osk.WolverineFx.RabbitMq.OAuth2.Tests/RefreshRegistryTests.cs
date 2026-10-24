using RabbitMQ.Client;
using TUnit.Mocks;
using Wolverine.RabbitMQ.Internal;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Tests;

/// <summary>Проверяет сопоставление транспортов и жизненный цикл их регистраций обновления токенов.</summary>
public class RefreshRegistryTests
{
    /// <summary>Проверяет возврат одного реестра при повторном обращении для одного транспорта.</summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task RepeatedLookupReturnsSameRegistry()
    {
        var transport = new RabbitMqTransport("rabbitmq");
        var registry = RabbitMqRefreshRegistryStore.GetOrCreate(transport);

        await Assert.That(RabbitMqRefreshRegistryStore.GetOrCreate(transport)).IsSameReferenceAs(registry);
    }

    /// <summary>Проверяет создание независимых реестров для разных экземпляров транспорта.</summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task DifferentTransportsHaveDifferentRegistries()
    {
        var first = RabbitMqRefreshRegistryStore.GetOrCreate(new RabbitMqTransport("rabbitmq"));
        var second = RabbitMqRefreshRegistryStore.GetOrCreate(new RabbitMqTransport("rabbitmq"));

        await Assert.That(ReferenceEquals(first, second)).IsFalse();
    }

    /// <summary>Проверяет возврат единственного реестра при конкурентном первом обращении.</summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task ConcurrentLookupsReturnSameRegistry()
    {
        var transport = new RabbitMqTransport("rabbitmq");
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var lookups = Enumerable.Range(0, 32).Select(_ => Task.Run(async () =>
        {
            await start.Task;
            return RabbitMqRefreshRegistryStore.GetOrCreate(transport);
        })).ToArray();

        start.SetResult();
        var registries = await Task.WhenAll(lookups).WaitAsync(TimeSpan.FromSeconds(10));

        await Assert.That(registries.All(registry => ReferenceEquals(registry, registries[0]))).IsTrue();
    }

    /// <summary>Проверяет отмену активного запроса и отклонение регистрации после начала остановки.</summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task CancelStopsRefreshAndRejectsNewRegistrations()
    {
        var registry = RabbitMqRefreshRegistryStore.GetOrCreate(new RabbitMqTransport("rabbitmq"));
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
        registry.Register(IConnection.Mock().Object, provider.Object);

        try
        {
            await requested.Task.WaitAsync(TimeSpan.FromSeconds(10));
            registry.Cancel();
            await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.That(() => registry.Register(IConnection.Mock().Object, provider.Object))
                .ThrowsExactly<ObjectDisposedException>();
        }
        finally
        {
            await registry.DisposeAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    /// <summary>Проверяет отклонение поздней регистрации сразу после Prefix пустого транспорта.</summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task PrefixStopsEmptyTransportBeforeOriginalDisposalCompletes()
    {
        var transport = new RabbitMqTransport("rabbitmq");
        RabbitMqTransportDisposeAsyncPatch.Prefix(transport);
        var registry = RabbitMqRefreshRegistryStore.GetOrCreate(transport);

        try
        {
            await Assert.That(() => registry.Register(IConnection.Mock().Object, ICredentialsProvider.Mock().Object))
                .ThrowsExactly<ObjectDisposedException>();
        }
        finally
        {
            await registry.DisposeAsync();
        }
    }

    /// <summary>Проверяет ожидание единственного освобождения активной регистрации всеми конкурентными вызовами.</summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task ConcurrentDisposalsAwaitSameCleanup()
    {
        var registry = RabbitMqRefreshRegistryStore.GetOrCreate(new RabbitMqTransport("rabbitmq"));
        var connection = IConnection.Mock();
        connection.IsOpen.Returns(true);
        var unsubscriptions = 0;
        connection.Events.ConnectionShutdownAsync.OnUnsubscribe(() => Interlocked.Increment(ref unsubscriptions));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task updateTask = Task.CompletedTask;
        connection.UpdateSecretAsync(Any<string>(), Any<string>(), Any<CancellationToken>())
            .Callback((string _, string _, CancellationToken cancellationToken) =>
            {
                updateTask = WaitForReleaseAsync(cancellationToken);
            })
            .Returns(() => updateTask);

        async Task WaitForReleaseAsync(CancellationToken cancellationToken)
        {
            using var callback = cancellationToken.Register(() => cancelled.TrySetResult());
            entered.TrySetResult();
            await release.Task;
            cancellationToken.ThrowIfCancellationRequested();
        }
        registry.Register(connection.Object, Provider().Object);

        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var disposals = Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
            {
                await start.Task;
                return registry.DisposeAsync();
            })).ToArray();
            start.SetResult();
            var cleanupTasks = await Task.WhenAll(disposals).WaitAsync(TimeSpan.FromSeconds(10));
            await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));

            await Assert.That(cleanupTasks.All(task => ReferenceEquals(task, cleanupTasks[0]) && !task.IsCompleted)).IsTrue();
            release.TrySetResult();
            await Task.WhenAll(cleanupTasks).WaitAsync(TimeSpan.FromSeconds(10));
            await registry.DisposeAsync().WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.That(unsubscriptions).IsEqualTo(1);
            await Assert.That(() => registry.Register(connection.Object, Provider().Object))
                .ThrowsExactly<ObjectDisposedException>();
        }
        finally
        {
            release.TrySetResult();
            await registry.DisposeAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    /// <summary>Проверяет освобождение реестра после ошибочного завершения исходной операции транспорта.</summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task PostfixDisposesRegistryAfterOriginalFailure()
    {
        var transport = new RabbitMqTransport("rabbitmq");
        var registry = RabbitMqRefreshRegistryStore.GetOrCreate(transport);
        var connection = IConnection.Mock();
        var unsubscriptions = 0;
        connection.Events.ConnectionShutdownAsync.OnUnsubscribe(() => Interlocked.Increment(ref unsubscriptions));
        registry.Register(connection.Object, Provider().Object);
        var original = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var result = new ValueTask(original.Task);

        try
        {
            RabbitMqTransportDisposeAsyncPatch.Prefix(transport);
            RabbitMqTransportDisposeAsyncPatch.Postfix(transport, ref result);
            await Assert.That(result.IsCompleted).IsFalse();
            var failure = new InvalidOperationException("transport disposal failed");
            original.SetException(failure);

            var actual = await Assert.That(async () => await result.AsTask().WaitAsync(TimeSpan.FromSeconds(10)))
                .ThrowsExactly<InvalidOperationException>();
            await Assert.That(actual).IsSameReferenceAs(failure);
            await Assert.That(registry.DisposeAsync().IsCompletedSuccessfully).IsTrue();
            await Assert.That(unsubscriptions).IsEqualTo(1);
        }
        finally
        {
            await registry.DisposeAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    /// <summary>Проверяет продолжение обновлений другого транспорта после освобождения первого.</summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task DisposingOneTransportKeepsOtherTransportRefreshing()
    {
        var firstTransport = new RabbitMqTransport("rabbitmq");
        var secondTransport = new RabbitMqTransport("rabbitmq");
        var first = RabbitMqRefreshRegistryStore.GetOrCreate(firstTransport);
        var second = RabbitMqRefreshRegistryStore.GetOrCreate(secondTransport);
        var firstConnection = IConnection.Mock();
        var secondConnection = IConnection.Mock();
        secondConnection.IsOpen.Returns(true);
        var firstUnsubscriptions = 0;
        var secondUnsubscriptions = 0;
        firstConnection.Events.ConnectionShutdownAsync.OnUnsubscribe(() => Interlocked.Increment(ref firstUnsubscriptions));
        secondConnection.Events.ConnectionShutdownAsync.OnUnsubscribe(() => Interlocked.Increment(ref secondUnsubscriptions));
        var updated = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var updatedAfterDisposal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstDisposed = 0;
        secondConnection.UpdateSecretAsync(Any<string>(), Any<string>(), Any<CancellationToken>())
            .Callback((string _, string _, CancellationToken cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                updated.TrySetResult();
                if (Volatile.Read(ref firstDisposed) == 1) updatedAfterDisposal.TrySetResult();
            })
            .ReturnsAsync(Task.CompletedTask);
        first.Register(firstConnection.Object, Provider().Object);
        second.Register(secondConnection.Object, Provider().Object);

        try
        {
            await updated.Task.WaitAsync(TimeSpan.FromSeconds(10));
            RabbitMqTransportDisposeAsyncPatch.Prefix(firstTransport);
            var disposal = ValueTask.CompletedTask;
            RabbitMqTransportDisposeAsyncPatch.Postfix(firstTransport, ref disposal);
            await disposal.AsTask().WaitAsync(TimeSpan.FromSeconds(10));
            Volatile.Write(ref firstDisposed, 1);

            await updatedAfterDisposal.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.That(firstUnsubscriptions).IsEqualTo(1);
            await Assert.That(secondUnsubscriptions).IsEqualTo(0);
        }
        finally
        {
            await Task.WhenAll(first.DisposeAsync(), second.DisposeAsync()).WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    /// <summary>Создаёт поставщика тестовых учётных данных с коротким периодом обновления.</summary>
    /// <returns>Подставной поставщик токенов.</returns>
    private static Mock<ICredentialsProvider> Provider()
    {
        var provider = ICredentialsProvider.Mock();
        provider.GetCredentialsAsync(Any<CancellationToken>())
            .Returns(() => Task.FromResult(new Credentials("oauth", "user", "test-token", TimeSpan.FromMilliseconds(150))));
        return provider;
    }
}
