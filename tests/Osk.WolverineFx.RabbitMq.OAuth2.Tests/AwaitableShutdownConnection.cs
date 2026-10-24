using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Tests;

/// <summary>
/// Дополняет подставное соединение возбуждением shutdown-события с ожиданием каждого обработчика.
/// </summary>
/// <param name="inner">Соединение TUnit.Mocks, которому передаются вызовы и изменения подписок.</param>
/// <remarks>
/// В TUnit.Mocks 1.74.0 сгенерированный RaiseConnectionShutdownAsync возвращает void и теряет задачи
/// AsyncEventHandler. Адаптер сохраняет эти задачи, не обращаясь к внутренним полям генератора.
/// </remarks>
internal sealed class AwaitableShutdownConnection(IConnection inner) : IConnection
{
    /// <summary>Обработчики, задачи которых необходимо ожидать при возбуждении события.</summary>
    private AsyncEventHandler<ShutdownEventArgs>? _shutdown;

    /// <inheritdoc/>
    public event AsyncEventHandler<ShutdownEventArgs> ConnectionShutdownAsync
    {
        add
        {
            inner.ConnectionShutdownAsync += value;
            _shutdown += value;
        }
        remove
        {
            inner.ConnectionShutdownAsync -= value;
            _shutdown -= value;
        }
    }

    /// <summary>Возбуждает событие и последовательно ожидает всех подписчиков из снимка списка.</summary>
    /// <param name="args">Причина закрытия соединения.</param>
    /// <returns>Задача завершения всех обработчиков; при отсутствии подписчиков завершается сразу.</returns>
    public async Task RaiseAsync(ShutdownEventArgs args)
    {
        var handlers = _shutdown;
        if (handlers is null) return;
        foreach (AsyncEventHandler<ShutdownEventArgs> handler in handlers.GetInvocationList())
        {
            await handler(this, args).ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    public Task UpdateSecretAsync(string newSecret, string reason, CancellationToken cancellationToken = default)
        => inner.UpdateSecretAsync(newSecret, reason, cancellationToken);

    /// <inheritdoc/>
    public Task CloseAsync(ushort reasonCode, string reasonText, TimeSpan timeout, bool abort, CancellationToken cancellationToken = default)
        => inner.CloseAsync(reasonCode, reasonText, timeout, abort, cancellationToken);

    /// <inheritdoc/>
    public Task<IChannel> CreateChannelAsync(CreateChannelOptions? options = null, CancellationToken cancellationToken = default)
        => inner.CreateChannelAsync(options, cancellationToken);

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => inner.DisposeAsync();

    /// <inheritdoc/>
    public void Dispose() => inner.Dispose();

    /// <inheritdoc/>
    public ushort ChannelMax => inner.ChannelMax;
    /// <inheritdoc/>
    public IDictionary<string, object?> ClientProperties => inner.ClientProperties;
    /// <inheritdoc/>
    public ShutdownEventArgs? CloseReason => inner.CloseReason;
    /// <inheritdoc/>
    public AmqpTcpEndpoint Endpoint => inner.Endpoint;
    /// <inheritdoc/>
    public uint FrameMax => inner.FrameMax;
    /// <inheritdoc/>
    public TimeSpan Heartbeat => inner.Heartbeat;
    /// <inheritdoc/>
    public bool IsOpen => inner.IsOpen;
    /// <inheritdoc/>
    public IProtocol Protocol => inner.Protocol;
    /// <inheritdoc/>
    public IDictionary<string, object?>? ServerProperties => inner.ServerProperties;
    /// <inheritdoc/>
    public IEnumerable<ShutdownReportEntry> ShutdownReport => inner.ShutdownReport;
    /// <inheritdoc/>
    public string? ClientProvidedName => inner.ClientProvidedName;
    /// <inheritdoc/>
    public int LocalPort => inner.LocalPort;
    /// <inheritdoc/>
    public int RemotePort => inner.RemotePort;

    /// <inheritdoc/>
    public event AsyncEventHandler<CallbackExceptionEventArgs> CallbackExceptionAsync
    {
        add => inner.CallbackExceptionAsync += value;
        remove => inner.CallbackExceptionAsync -= value;
    }

    /// <inheritdoc/>
    public event AsyncEventHandler<AsyncEventArgs> RecoverySucceededAsync
    {
        add => inner.RecoverySucceededAsync += value;
        remove => inner.RecoverySucceededAsync -= value;
    }

    /// <inheritdoc/>
    public event AsyncEventHandler<ConnectionRecoveryErrorEventArgs> ConnectionRecoveryErrorAsync
    {
        add => inner.ConnectionRecoveryErrorAsync += value;
        remove => inner.ConnectionRecoveryErrorAsync -= value;
    }

    /// <inheritdoc/>
    public event AsyncEventHandler<ConsumerTagChangedAfterRecoveryEventArgs> ConsumerTagChangeAfterRecoveryAsync
    {
        add => inner.ConsumerTagChangeAfterRecoveryAsync += value;
        remove => inner.ConsumerTagChangeAfterRecoveryAsync -= value;
    }

    /// <inheritdoc/>
    public event AsyncEventHandler<QueueNameChangedAfterRecoveryEventArgs> QueueNameChangedAfterRecoveryAsync
    {
        add => inner.QueueNameChangedAfterRecoveryAsync += value;
        remove => inner.QueueNameChangedAfterRecoveryAsync -= value;
    }

    /// <inheritdoc/>
    public event AsyncEventHandler<RecoveringConsumerEventArgs> RecoveringConsumerAsync
    {
        add => inner.RecoveringConsumerAsync += value;
        remove => inner.RecoveringConsumerAsync -= value;
    }

    /// <inheritdoc/>
    public event AsyncEventHandler<ConnectionBlockedEventArgs> ConnectionBlockedAsync
    {
        add => inner.ConnectionBlockedAsync += value;
        remove => inner.ConnectionBlockedAsync -= value;
    }

    /// <inheritdoc/>
    public event AsyncEventHandler<AsyncEventArgs> ConnectionUnblockedAsync
    {
        add => inner.ConnectionUnblockedAsync += value;
        remove => inner.ConnectionUnblockedAsync -= value;
    }
}
