using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Wolverine.RabbitMQ.Internal;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Patch;

internal sealed class RabbitMqRefreshRegistry(ILogger logger)
{
    private static readonly ConditionalWeakTable<RabbitMqTransport, RabbitMqRefreshRegistry> Registries = new();
    private readonly Lock _sync = new();
    private readonly List<RabbitMqCredentialsRefreshRegistration> _refreshers = [];
    private Task? _disposeTask;
    private bool _stopping;

    public static RabbitMqRefreshRegistry For(RabbitMqTransport transport) =>
        Registries.GetValue(transport, static t => new RabbitMqRefreshRegistry(t.Logger));

    public void Register(IConnection connection, ICredentialsProvider provider)
    {
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_stopping, this);
            var refresher = new RabbitMqCredentialsRefreshRegistration(connection, provider, logger);
            refresher.Start();
            _refreshers.Add(refresher);
        }
    }

    public static void TryCancel(RabbitMqTransport transport)
    {
        var registry = For(transport);
        lock (registry._sync)
        {
            registry._stopping = true;
            foreach (var refresher in registry._refreshers) refresher.Cancel();
        }
    }

    public static ValueTask TryDisposeAsync(RabbitMqTransport transport)
    {
        // Keep a stopped entry so a late connection cannot restart refresh after shutdown.
        // The weak transport key still permits collection of the registry.
        return Registries.TryGetValue(transport, out var registry)
            ? new ValueTask(registry.DisposeAsync())
            : ValueTask.CompletedTask;
    }

    private Task DisposeAsync()
    {
        lock (_sync)
        {
            if (_disposeTask is not null) return _disposeTask;
            _stopping = true;
            var refreshers = _refreshers.ToArray();
            _refreshers.Clear();
            foreach (var refresher in refreshers) refresher.Cancel();
            _disposeTask = Task.WhenAll(refreshers.Select(x => x.DisposeAsync().AsTask()));
            return _disposeTask;
        }
    }
}
