using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Tests;

/// <summary>Проверяет необходимые для миграции особенности событий TUnit.Mocks 1.74.0.</summary>
public class MockCompatibilityTests
{
    /// <summary>Проверяет ожидание всех обработчиков, передачу их ошибок и возбуждение события без подписчиков.</summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task ShutdownWaitsForEveryHandlerAndPropagatesAsyncFailure()
    {
        var connection = IConnection.Mock();
        var shutdown = new AwaitableShutdownConnection(connection.Object);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failure = new InvalidOperationException("shutdown handler failed");
        var secondCalled = false;
        AsyncEventHandler<ShutdownEventArgs> first = (_, _) => release.Task;
        AsyncEventHandler<ShutdownEventArgs> second = async (_, _) =>
        {
            secondCalled = true;
            await Task.Yield();
            throw failure;
        };
        var args = new ShutdownEventArgs(ShutdownInitiator.Application, 200, "application stopping");
        await Assert.That(connection.Events.ConnectionShutdownAsync.SubscriberCount).IsEqualTo(0);
        shutdown.ConnectionShutdownAsync += first;
        shutdown.ConnectionShutdownAsync += second;
        await Assert.That(connection.Events.ConnectionShutdownAsync.SubscriberCount).IsEqualTo(2);

        var raised = shutdown.RaiseAsync(args);
        try
        {
            await Assert.That(raised.IsCompleted).IsFalse();
            await Assert.That(secondCalled).IsFalse();
        }
        finally
        {
            release.TrySetResult();
        }

        var actual = await Assert.That(async () => await raised.WaitAsync(TimeSpan.FromSeconds(10)))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(actual).IsSameReferenceAs(failure);
        await Assert.That(secondCalled).IsTrue();
        shutdown.ConnectionShutdownAsync -= first;
        shutdown.ConnectionShutdownAsync -= second;
        await Assert.That(connection.Events.ConnectionShutdownAsync.SubscriberCount).IsEqualTo(0);
        await shutdown.RaiseAsync(args).WaitAsync(TimeSpan.FromSeconds(10));
    }

    /// <summary>Подтверждает, что OnUnsubscribe выбрасывает исходное исключение непосредственно из оператора отписки.</summary>
    /// <returns>Задача выполнения проверки.</returns>
    [Test]
    public async Task UnsubscribeCallbackThrowsFromEventRemove()
    {
        var connection = IConnection.Mock();
        var failure = new ObjectDisposedException("AutorecoveringConnection");
        var attempts = 0;
        connection.Events.ConnectionShutdownAsync.OnUnsubscribe(() =>
        {
            Interlocked.Increment(ref attempts);
            throw failure;
        });
        AsyncEventHandler<ShutdownEventArgs> handler = static (_, _) => Task.CompletedTask;
        connection.Object.ConnectionShutdownAsync += handler;

        var actual = await Assert.That(() => { connection.Object.ConnectionShutdownAsync -= handler; })
            .ThrowsExactly<ObjectDisposedException>();

        await Assert.That(actual).IsSameReferenceAs(failure);
        await Assert.That(attempts).IsEqualTo(1);
    }
}
