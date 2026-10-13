using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.OAuth2;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Patch;

// Binds the native refresh scheduler to a connection and the transport lifetime.
internal sealed class RabbitMqCredentialsRefreshRegistration(
    IConnection connection,
    ICredentialsProvider provider,
    ILogger logger) : IAsyncDisposable
{
    private readonly Lock _sync = new();
    private readonly CancellationTokenSource _cts = new();
    private CredentialsRefresher? _refresher;
    private Task? _disposeTask;

    public void Start()
    {
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_disposeTask is not null, this);
            if (_refresher is not null)
                throw new InvalidOperationException("Credentials refresher has already started");

            connection.ConnectionShutdownAsync += OnShutdownAsync;
            try
            {
                _refresher = new CredentialsRefresher(provider, OnRefreshedAsync, _cts.Token);
            }
            catch
            {
                connection.ConnectionShutdownAsync -= OnShutdownAsync;
                _disposeTask = Task.CompletedTask;
                try { _cts.Cancel(); }
                finally { _cts.Dispose(); }
                throw;
            }
        }
    }

    private Task OnShutdownAsync(object sender, ShutdownEventArgs args)
    {
        if (args.Initiator == ShutdownInitiator.Application) Cancel();
        return Task.CompletedTask;
    }

    private async Task OnRefreshedAsync(Credentials? credentials, Exception? exception, CancellationToken ct)
    {
        if (ct.IsCancellationRequested) return;
        if (exception is not null)
        {
            logger.LogError(exception, "RabbitMQ OAuth2 credentials refresh failed");
            return;
        }

        if (credentials is null || string.IsNullOrWhiteSpace(credentials.Password))
        {
            logger.LogError("RabbitMQ OAuth2 provider returned an empty token");
            return;
        }

        if (!connection.IsOpen) return;
        try
        {
            await connection.UpdateSecretAsync(credentials.Password, "OAuth2 access token refresh", ct)
                .ConfigureAwait(false);
            logger.LogDebug("RabbitMQ OAuth2 token refreshed successfully");
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex)
        {
            logger.LogError(ex, "RabbitMQ OAuth2 connection token update failed");
        }
    }

    public void Cancel()
    {
        lock (_sync)
        {
            if (_disposeTask is null) _cts.Cancel();
        }
    }

    public ValueTask DisposeAsync()
    {
        lock (_sync)
        {
            if (_disposeTask is not null) return new ValueTask(_disposeTask);
            _cts.Cancel();
            _disposeTask = Task.Run(() =>
            {
                try { _refresher?.Dispose(); }
                // OAuth2 2.0.0 throws an aggregate if cancellation interrupts its delay.
                catch (AggregateException ex) when (_cts.IsCancellationRequested &&
                    ex.Flatten().InnerExceptions.All(e => e is OperationCanceledException)) { }
                finally
                {
                    connection.ConnectionShutdownAsync -= OnShutdownAsync;
                    _cts.Dispose();
                }
            });
            return new ValueTask(_disposeTask);
        }
    }
}
