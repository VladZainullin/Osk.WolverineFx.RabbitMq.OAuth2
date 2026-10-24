using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.OAuth2;

namespace Osk.WolverineFx.RabbitMq.OAuth2;

internal sealed class ConnectionCredentialsRefresherRelation : IAsyncDisposable
{
    private readonly IConnection _connection;
    private readonly ILogger _logger;
    private readonly Lock _lock = new();
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly CredentialsRefresher _refresher;
    private Task? _disposeTask;

    public ConnectionCredentialsRefresherRelation(IConnection connection, ICredentialsProvider provider, ILogger logger)
    {
        _connection = connection;
        _logger = logger;
        _connection.ConnectionShutdownAsync += OnShutdownAsync;
        try
        {
            _refresher = new CredentialsRefresher(provider, OnRefreshedAsync, _cancellationTokenSource.Token);
        }
        catch
        {
            try
            {
                Cancel();
            }
            finally
            {
                _disposeTask = Task.CompletedTask;
                _cancellationTokenSource.Dispose();
            }

            throw;
        }
    }

    private Task OnShutdownAsync(object sender, ShutdownEventArgs args)
    {
        if (args.Initiator == ShutdownInitiator.Application) Cancel();
        return Task.CompletedTask;
    }

    private async Task OnRefreshedAsync(Credentials? credentials, Exception? exception,
        CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested) return;
        if (exception is not null)
        {
            _logger.LogError(exception, "RabbitMQ OAuth2 credentials refresh failed");
            return;
        }

        if (credentials is null || string.IsNullOrWhiteSpace(credentials.Password))
        {
            _logger.LogError("RabbitMQ OAuth2 provider returned an empty token");
            return;
        }

        if (!_connection.IsOpen) return;
        try
        {
            await _connection.UpdateSecretAsync(credentials.Password, "OAuth2 access token refresh", cancellationToken)
                .ConfigureAwait(false);
            _logger.LogDebug("RabbitMQ OAuth2 token refreshed successfully");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "RabbitMQ OAuth2 connection token update failed");
        }
    }

    public void Cancel()
    {
        lock (_lock)
        {
            if (_disposeTask is not null || _cancellationTokenSource.IsCancellationRequested) return;
            // The transport prefix calls this while the connection still permits event access.
            // An externally disposed AutorecoveringConnection rejects event unsubscription.
            try
            {
                _connection.ConnectionShutdownAsync -= OnShutdownAsync;
            }
            catch (ObjectDisposedException)
            {
            }

            _cancellationTokenSource.Cancel();
        }
    }

    public ValueTask DisposeAsync()
    {
        lock (_lock)
        {
            if (_disposeTask is not null) return new ValueTask(_disposeTask);
            Cancel();
            _disposeTask = Task.Run(DisposeRefresher);
            return new ValueTask(_disposeTask);
        }
    }

    private void DisposeRefresher()
    {
        try
        {
            _refresher.Dispose();
        }
        // OAuth2 2.0.0 throws an aggregate if cancellation interrupts its delay.
        catch (AggregateException ex) when (_cancellationTokenSource.IsCancellationRequested &&
                                            ex.Flatten().InnerExceptions
                                                .All(static e => e is OperationCanceledException))
        {
        }
        finally
        {
            _cancellationTokenSource.Dispose();
        }
    }
}