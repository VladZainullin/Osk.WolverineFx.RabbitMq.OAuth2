using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.OAuth2;
using Wolverine;
using Wolverine.RabbitMQ;
using Wolverine.RabbitMQ.Internal;
using Wolverine.Runtime;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Patch.IntegrationTests;

internal sealed class WolverineSession : IAsyncDisposable
{
    private readonly IOAuth2Client _oauthClient;
    private readonly HttpClientHandler _httpHandler;
    private readonly OAuth2Environment _environment;
    private readonly IHost _host;
    private readonly DeliveryProbe _delivery = new();
    private bool _started;
    private Task? _disposeTask;
    public string ConnectionName { get; } = $"wolverine-oauth2-it-{Guid.NewGuid():N}";
    public string QueueName { get; } = $"oauth2-it-{Guid.NewGuid():N}";
    public TrackingCredentialsProvider Credentials { get; }
    public RefreshLog Log { get; } = new();

    private WolverineSession(OAuth2Environment environment, IOAuth2Client oauthClient, HttpClientHandler httpHandler, bool invalidToken)
    {
        _oauthClient = oauthClient;
        _environment = environment;
        _httpHandler = httpHandler;
        Credentials = new TrackingCredentialsProvider(
            new OAuth2ClientCredentialsProvider(ConnectionName, oauthClient), invalidToken);
        _host = Host.CreateDefaultBuilder()
            .ConfigureLogging(logging => logging.ClearProviders().SetMinimumLevel(LogLevel.Debug).AddProvider(Log))
            .UseWolverine(options =>
            {
                options.UseRuntimeCompilation();
                options.Services.AddSingleton(_delivery);
                options.Discovery.DisableConventionalDiscovery();
                options.Discovery.IncludeType<ProbeMessageHandler>();
                options.UseRabbitMq(factory =>
                {
                    factory.HostName = environment.RabbitMq.Hostname;
                    factory.Port = environment.RabbitMq.GetMappedPublicPort(5672);
                    factory.CredentialsProvider = Credentials;
                    factory.ClientProvidedName = ConnectionName;
                    factory.RequestedConnectionTimeout = TimeSpan.FromSeconds(10);
                }).AutoProvision();
                options.ListenToRabbitQueue(QueueName);
                options.PublishMessage<ProbeMessage>().ToRabbitQueue(QueueName);
            }).Build();
    }

    public static async Task<WolverineSession> CreateAsync(OAuth2Environment environment, bool invalidToken = false, CancellationToken ct = default)
    {
        var handler = environment.Tls.CreateHandler();
        try
        {
            var oauth = await new OAuth2ClientBuilder("wolverine", environment.ClientSecret, tokenEndpoint: environment.TokenEndpoint)
                .SetHttpClientHandler(handler).BuildAsync(ct);
            return new WolverineSession(environment, oauth, handler, invalidToken);
        }
        catch { handler.Dispose(); throw; }
    }

    public async Task StartAsync(CancellationToken ct = default)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(45));
        await _host.StartAsync(timeout.Token);
        _started = true;
        await OAuth2Environment.WaitUntilAsync(() => Task.FromResult(Log.Successes >= 2), TimeSpan.FromSeconds(15), ct);
    }

    public async Task SendAndReceiveAsync(CancellationToken ct = default)
    {
        var before = await _environment.DeliveredMessagesAsync(QueueName, ct);
        var id = Guid.NewGuid();
        var received = _delivery.Expect(id);
        await _host.Services.GetRequiredService<IMessageBus>().PublishAsync(new ProbeMessage(id));
        await received.WaitAsync(TimeSpan.FromSeconds(15), ct);
        // Prove delivery went through RabbitMQ, even if local routing is accidentally enabled.
        await OAuth2Environment.WaitUntilAsync(async () => await _environment.DeliveredMessagesAsync(QueueName, ct) > before,
            TimeSpan.FromSeconds(15), ct);
    }

    public Task[] CaptureRefreshTasks()
    {
        var transport = _host.Services.GetRequiredService<IWolverineRuntime>().Options.Transports.OfType<RabbitMqTransport>().Single();
        var registry = RabbitMqRefreshRegistry.For(transport);
        // OAuth2 2.0.0 exposes no worker completion API. Read the pinned implementation
        // only in this diagnostic so shutdown asserts actual task completion, not just silence.
        return ReadField<List<RabbitMqCredentialsRefreshRegistration>>(registry, "_refreshers")
            .Select(registration => ReadField<CredentialsRefresher>(registration, "_refresher"))
            .Select(refresher => ReadField<Task>(refresher, "_refreshTask")).ToArray();
    }

    private static T ReadField<T>(object instance, string name) =>
        (T)(instance.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)?.GetValue(instance)
            ?? throw new MissingFieldException(instance.GetType().FullName, name));

    public ValueTask DisposeAsync() => new(_disposeTask ??= DisposeCoreAsync());

    private async Task DisposeCoreAsync()
    {
        try
        {
            if (_started)
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                await _host.StopAsync(timeout.Token);
            }
        }
        finally
        {
            try
            {
                if (_host is IAsyncDisposable asyncHost) await asyncHost.DisposeAsync();
                else _host.Dispose();
            }
            finally
            {
                Credentials.Dispose();
                (_oauthClient as IDisposable)?.Dispose();
                _httpHandler.Dispose();
            }
        }
    }
}

internal sealed class TrackingCredentialsProvider(OAuth2ClientCredentialsProvider inner, bool invalidToken) : ICredentialsProvider, IDisposable
{
    private int _requests, _active, _failures;
    private readonly ConcurrentQueue<DateTimeOffset> _expirations = new();
    public string Name => inner.Name;
    public int Requests => Volatile.Read(ref _requests);
    public int ActiveRequests => Volatile.Read(ref _active);
    public int Failures => Volatile.Read(ref _failures);
    public DateTimeOffset FirstExpiration => _expirations.First();

    public async Task<Credentials> GetCredentialsAsync(CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _requests);
        Interlocked.Increment(ref _active);
        try
        {
            var credentials = await inner.GetCredentialsAsync(cancellationToken);
            // Read expiry for the timing oracle; RabbitMQ independently validates signature/audience.
            var payload = credentials.Password.Split('.')[1].Replace('-', '+').Replace('_', '/');
            payload = payload.PadRight((payload.Length + 3) / 4 * 4, '=');
            using var jwt = JsonDocument.Parse(Convert.FromBase64String(payload));
            _expirations.Enqueue(DateTimeOffset.FromUnixTimeSeconds(jwt.RootElement.GetProperty("exp").GetInt64()));
            if (!invalidToken) return credentials;
            var parts = credentials.Password.Split('.');
            var signature = parts[2].ToCharArray();
            signature[0] = signature[0] == 'A' ? 'B' : 'A';
            // Keep valid JSON, issuer, audience and expiry; only the signature is invalid.
            return new Credentials(Name, credentials.UserName,
                $"{parts[0]}.{parts[1]}.{new string(signature)}", credentials.ValidUntil);
        }
        catch
        {
            Interlocked.Increment(ref _failures);
            throw;
        }
        finally { Interlocked.Decrement(ref _active); }
    }

    public void Dispose() => inner.Dispose();
}

internal sealed class RefreshLog : ILoggerProvider
{
    private int _successes, _errors;
    public int Successes => Volatile.Read(ref _successes);
    public int Errors => Volatile.Read(ref _errors);
    public ILogger CreateLogger(string categoryName) => new CounterLogger(this);
    public void Dispose() { }

    private sealed class CounterLogger(RefreshLog owner) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel level, EventId id, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            if (message == "RabbitMQ OAuth2 token refreshed successfully") Interlocked.Increment(ref owner._successes);
            if (message is "RabbitMQ OAuth2 credentials refresh failed" or "RabbitMQ OAuth2 connection token update failed")
                Interlocked.Increment(ref owner._errors);
        }
    }
}

public sealed record ProbeMessage(Guid Id);

public sealed class DeliveryProbe
{
    private readonly ConcurrentDictionary<Guid, TaskCompletionSource> _pending = new();
    public Task Expect(Guid id)
    {
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_pending.TryAdd(id, completion)) throw new InvalidOperationException("Duplicate message id");
        return completion.Task;
    }
    public void Received(Guid id)
    {
        if (_pending.TryRemove(id, out var completion)) completion.TrySetResult();
    }
}

public sealed class ProbeMessageHandler
{
    public static void Handle(ProbeMessage message, DeliveryProbe probe) => probe.Received(message.Id);
}
