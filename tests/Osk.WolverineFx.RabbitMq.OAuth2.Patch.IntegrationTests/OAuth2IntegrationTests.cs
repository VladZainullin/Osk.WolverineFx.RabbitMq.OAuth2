using RabbitMQ.Client.Exceptions;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Patch.IntegrationTests;

[NotInParallel]
[Category("Integration")]
[Timeout(180_000)]
public sealed class OAuth2IntegrationTests
{
    private static OAuth2Environment? _environment;
    private static OAuth2Environment Environment => _environment ?? throw new InvalidOperationException("Containers have not started");

    [Before(HookType.Class)]
    [Timeout(300_000)]
    public static async Task StartContainers(CancellationToken ct)
    {
        _environment = new OAuth2Environment();
        await _environment.StartAsync(ct);
    }

    [After(HookType.Class)]
    public static async Task StopContainers(CancellationToken ct)
    {
        if (_environment is not null) await _environment.DisposeAsync();
    }

    [Test]
    public async Task ConnectsWithValidToken(CancellationToken ct)
    {
        await using var session = await WolverineSession.CreateAsync(Environment, ct: ct);
        await session.StartAsync(ct);
        await session.SendAndReceiveAsync(ct);
        await Assert.That(session.Credentials.Requests >= 2).IsTrue();
        await WaitForConnectionsAsync(session, ct);
        await Assert.That((await Environment.ConnectionIdsAsync(session.ConnectionName)).Length).IsEqualTo(2);
    }

    [Test]
    public async Task RejectsInvalidToken(CancellationToken ct)
    {
        await using var session = await WolverineSession.CreateAsync(Environment, invalidToken: true, ct: ct);
        var failure = await Assert.That(async () => await session.StartAsync(ct)).Throws<Exception>();
        await Assert.That(ExceptionChain(failure!).Any(e => e is AuthenticationFailureException or PossibleAuthenticationFailureException)).IsTrue();
        await OAuth2Environment.WaitUntilAsync(async () => (await Environment.ConnectionIdsAsync(session.ConnectionName)).Length == 0,
            TimeSpan.FromSeconds(10), ct);
        await Assert.That(session.Log.Successes).IsEqualTo(0);
    }

    [Test]
    public async Task RefreshesExpiredToken(CancellationToken ct)
    {
        await using var session = await WolverineSession.CreateAsync(Environment, ct: ct);
        await session.StartAsync(ct);
        await session.SendAndReceiveAsync(ct);
        var initialRefreshes = session.Log.Successes;
        await WaitPastInitialExpiryAsync(session, ct);
        await OAuth2Environment.WaitUntilAsync(() => Task.FromResult(session.Log.Successes > initialRefreshes), TimeSpan.FromSeconds(15), ct);
        // Distinct messages after the original JWT has expired must traverse the real broker.
        for (var i = 0; i < 3; i++) await session.SendAndReceiveAsync(ct);
    }

    [Test]
    public async Task PreservesConnectionAfterRefresh(CancellationToken ct)
    {
        await using var session = await WolverineSession.CreateAsync(Environment, ct: ct);
        await session.StartAsync(ct);
        await WaitForConnectionsAsync(session, ct);
        var before = await Environment.ConnectionIdsAsync(session.ConnectionName);
        await Assert.That(before.Length).IsEqualTo(2);
        await WaitPastInitialExpiryAsync(session, ct);
        await session.SendAndReceiveAsync(ct);
        var after = await Environment.ConnectionIdsAsync(session.ConnectionName);
        // Broker-side name + connected_at distinguishes recovery/reconnect from update-secret.
        await Assert.That(after.SequenceEqual(before)).IsTrue();
    }

    [Test]
    public async Task RecoversAfterKeycloakOutage(CancellationToken ct)
    {
        await using var session = await WolverineSession.CreateAsync(Environment, ct: ct);
        await session.StartAsync(ct);
        await session.SendAndReceiveAsync(ct);
        var failures = session.Credentials.Failures;
        var errors = session.Log.Errors;
        await Environment.Keycloak.StopAsync(ct);
        try
        {
            await OAuth2Environment.WaitUntilAsync(() => Task.FromResult(session.Credentials.Failures > failures && session.Log.Errors > errors),
                TimeSpan.FromSeconds(25), ct);
        }
        finally
        {
            await Environment.Keycloak.StartAsync();
            await Environment.WaitForKeycloakAsync(ct);
        }

        var refreshes = session.Log.Successes;
        await OAuth2Environment.WaitUntilAsync(() => Task.FromResult(session.Log.Successes > refreshes), TimeSpan.FromSeconds(60), ct);
        await session.SendAndReceiveAsync(ct);
    }

    [Test]
    public async Task StopsRefreshingAfterDispose(CancellationToken ct)
    {
        await using var session = await WolverineSession.CreateAsync(Environment, ct: ct);
        await session.StartAsync(ct);
        await session.SendAndReceiveAsync(ct);
        var workers = session.CaptureRefreshTasks();
        await Assert.That(workers.Length).IsEqualTo(2);
        await session.DisposeAsync();
        await Assert.That(workers.All(task => task.IsCompleted)).IsTrue();
        await Assert.That(session.Credentials.ActiveRequests).IsEqualTo(0);
        var requests = session.Credentials.Requests;
        var refreshes = session.Log.Successes;
        await OAuth2Environment.WaitUntilAsync(async () => (await Environment.ConnectionIdsAsync(session.ConnectionName)).Length == 0,
            TimeSpan.FromSeconds(10), ct);
        // Observe two full token lifetimes, longer than multiple native refresh intervals.
        await Task.Delay(TimeSpan.FromSeconds(OAuth2Environment.TokenLifetimeSeconds * 2), ct);
        await Assert.That(session.Credentials.Requests).IsEqualTo(requests);
        await Assert.That(session.Log.Successes).IsEqualTo(refreshes);
        await Assert.That(session.Credentials.ActiveRequests).IsEqualTo(0);
    }

    private static async Task WaitPastInitialExpiryAsync(WolverineSession session, CancellationToken ct)
    {
        var remaining = session.Credentials.FirstExpiration.AddSeconds(2) - DateTimeOffset.UtcNow;
        if (remaining > TimeSpan.Zero) await Task.Delay(remaining, ct);
        await Assert.That(DateTimeOffset.UtcNow > session.Credentials.FirstExpiration).IsTrue();
    }

    private static Task WaitForConnectionsAsync(WolverineSession session, CancellationToken ct) =>
        OAuth2Environment.WaitUntilAsync(async () => (await Environment.ConnectionIdsAsync(session.ConnectionName)).Length == 2,
            TimeSpan.FromSeconds(15), ct);

    private static IEnumerable<Exception> ExceptionChain(Exception exception)
    {
        yield return exception;
        if (exception is AggregateException aggregate)
        {
            foreach (var inner in aggregate.InnerExceptions.SelectMany(ExceptionChain)) yield return inner;
        }
        else if (exception.InnerException is { } inner)
        {
            foreach (var nested in ExceptionChain(inner)) yield return nested;
        }
    }
}
