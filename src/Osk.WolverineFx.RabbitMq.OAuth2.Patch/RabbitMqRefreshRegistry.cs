using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Wolverine.RabbitMQ.Internal;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Patch;

internal sealed class RabbitMqRefreshRegistry(ILogger logger)
{
    private static readonly ConditionalWeakTable<RabbitMqTransport, RabbitMqRefreshRegistry> Registries = new();
    private readonly Lock _sync = new();
    private readonly List<RabbitMqTokenRefresher> _refreshers = [];
    private bool _stopping;

    public static RabbitMqRefreshRegistry For(RabbitMqTransport transport) =>
        Registries.GetValue(transport, static t => new RabbitMqRefreshRegistry(t.Logger));

    public void Register(IConnection connection, ICredentialsProvider provider)
    {
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_stopping, this);
            var refresher = new RabbitMqTokenRefresher(connection, provider, logger);
            refresher.Start();
            _refreshers.Add(refresher);
        }
    }

    public static void TryCancel(RabbitMqTransport transport)
    {
        if (!Registries.TryGetValue(transport, out var registry)) return;
        lock (registry._sync)
        {
            registry._stopping = true;
            foreach (var refresher in registry._refreshers) refresher.Cancel();
        }
    }

    public static async ValueTask TryDisposeAsync(RabbitMqTransport transport)
    {
        if (!Registries.TryGetValue(transport, out var registry)) return;
        try { await registry.DisposeAsync().ConfigureAwait(false); }
        finally { Registries.Remove(transport); }
    }

    private async ValueTask DisposeAsync()
    {
        RabbitMqTokenRefresher[] refreshers;
        lock (_sync)
        {
            _stopping = true;
            refreshers = [.. _refreshers];
            _refreshers.Clear();
        }
        foreach (var refresher in refreshers) refresher.Cancel();
        await Task.WhenAll(refreshers.Select(x => x.DisposeAsync().AsTask())).ConfigureAwait(false);
    }
}
