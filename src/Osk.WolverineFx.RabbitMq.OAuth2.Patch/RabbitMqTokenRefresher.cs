using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Patch;

internal sealed class RabbitMqTokenRefresher(
    IConnection connection,
    ICredentialsProvider provider,
    ILogger logger) : IAsyncDisposable
{
    private readonly Lock _sync = new();
    private readonly CancellationTokenSource _cts = new();

    private Task _task = Task.CompletedTask;
    private bool _started;
    private bool _disposed;

    public void Start()
    {
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_started)
            {
                throw new InvalidOperationException(
                    "Token refresher has already started");
            }

            _started = true;
            connection.ConnectionShutdownAsync += OnShutdownAsync;
            _task = Task.Run(RefreshAsync);
        }
    }

    private Task OnShutdownAsync(
        object sender,
        ShutdownEventArgs args)
    {
        if (args.Initiator == ShutdownInitiator.Application)
        {
            Cancel();
        }

        return Task.CompletedTask;
    }

    private async Task RefreshAsync()
    {
        var ct = _cts.Token;
        var failures = 0;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (!connection.IsOpen)
                {
                    await Task.Delay(
                        TimeSpan.FromSeconds(5), ct);

                    continue;
                }

                var credentials = await provider
                    .GetCredentialsAsync(ct)
                    .ConfigureAwait(false);

                if (string.IsNullOrWhiteSpace(credentials.Password))
                {
                    throw new InvalidOperationException(
                        "OAuth2 provider returned an empty token");
                }

                var delay = GetRefreshDelay(credentials);

                if (connection.IsOpen)
                {
                    await connection.UpdateSecretAsync(
                        credentials.Password,
                        "OAuth2 access token refresh",
                        ct).ConfigureAwait(false);

                    logger.LogDebug(
                        "RabbitMQ OAuth2 token refreshed successfully");
                }

                failures = 0;

                await Task.Delay(delay, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                if (ct.IsCancellationRequested)
                {
                    return;
                }

                failures = Math.Min(failures + 1, 5);

                logger.LogError(
                    ex,
                    "RabbitMQ OAuth2 refresh failed. Attempt {Attempt}",
                    failures);

                var seconds = Math.Min(
                    60,
                    5 * (1 << (failures - 1)));

                try
                {
                    await Task.Delay(
                        TimeSpan.FromSeconds(seconds),
                        ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                    when (ct.IsCancellationRequested)
                {
                    return;
                }
            }
        }
    }

    private static TimeSpan GetRefreshDelay(Credentials credentials)
    {
        var ttl = credentials.ValidUntil
                  ?? throw new InvalidOperationException(
                      "OAuth2 token expiration is not specified");

        if (ttl <= TimeSpan.FromSeconds(10))
        {
            throw new InvalidOperationException(
                "OAuth2 token lifetime is too short");
        }

        return TimeSpan.FromTicks(
            Math.Min(
                (long)(ttl.Ticks * 0.6),
                TimeSpan.FromMinutes(30).Ticks));
    }

    public void Cancel()
    {
        lock (_sync)
        {
            if (!_disposed)
            {
                _cts.Cancel();
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        Task task;

        lock (_sync)
        {
            if (_disposed)
            {
                return;
            }

            _cts.Cancel();
            task = _task;
        }

        try
        {
            await task.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (_cts.IsCancellationRequested)
        {
            // Expected cancellation.
        }
        finally
        {
            lock (_sync)
            {
                if (!_disposed)
                {
                    _disposed = true;
                    connection.ConnectionShutdownAsync -= OnShutdownAsync;
                    _cts.Dispose();
                }
            }
        }
    }
}