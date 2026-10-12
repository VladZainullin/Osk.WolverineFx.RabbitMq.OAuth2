using HarmonyLib;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Wolverine.RabbitMQ.Internal;
using Xunit;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Patch.Tests;

public class PatchTests
{
    [Fact]
    public void InstallsBothPatchesExactlyOnce()
    {
        RabbitMqOAuth2Patches.Install();
        RabbitMqOAuth2Patches.Install();
        var create = AccessTools.DeclaredMethod(typeof(RabbitMqTransport), "CreateConnectionAsync");
        var dispose = AccessTools.DeclaredMethod(typeof(RabbitMqTransport), nameof(RabbitMqTransport.DisposeAsync));
        Assert.Single(Harmony.GetPatchInfo(create)!.Postfixes, p => p.owner == RabbitMqOAuth2Patches.HarmonyId);
        Assert.Single(Harmony.GetPatchInfo(dispose)!.Prefixes, p => p.owner == RabbitMqOAuth2Patches.HarmonyId);
        Assert.Single(Harmony.GetPatchInfo(dispose)!.Postfixes, p => p.owner == RabbitMqOAuth2Patches.HarmonyId);
    }

    [Fact]
    public async Task CreatedConnectionRefreshesAndPatchedTransportDisposalStopsWorker()
    {
        RabbitMqOAuth2Patches.Install();
        var transport = new RabbitMqTransport("rabbitmq");
        var connection = new Mock<IConnection>();
        connection.SetupGet(x => x.IsOpen).Returns(true);
        var updated = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        connection.Setup(x => x.UpdateSecretAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns((string token, string reason, CancellationToken ct) => { updated.TrySetResult(token); return Task.CompletedTask; });
        var provider = Provider("new-token");
        typeof(RabbitMqTransport).GetProperty(nameof(RabbitMqTransport.ConnectionFactory))!
            .SetValue(transport, new ConnectionFactory { CredentialsProvider = provider.Object });
        var original = Task.FromResult(connection.Object);
        RabbitMqConnectionPatch.Postfix(transport, ref original);
        var result = await original;
        Assert.Same(connection.Object, result);
        Assert.Equal("new-token", await updated.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        await transport.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        provider.Verify(x => x.GetCredentialsAsync(It.IsAny<CancellationToken>()), Times.Once);
        connection.VerifyRemove(x => x.ConnectionShutdownAsync -= It.IsAny<RabbitMQ.Client.Events.AsyncEventHandler<ShutdownEventArgs>>(), Times.Once);
    }

    [Fact]
    public void PasswordOnlyConnectionIsNotWrapped()
    {
        var transport = new RabbitMqTransport("rabbitmq");
        typeof(RabbitMqTransport).GetProperty(nameof(RabbitMqTransport.ConnectionFactory))!
            .SetValue(transport, new ConnectionFactory { CredentialsProvider = new BasicCredentialsProvider("basic", "user", "password") });
        var original = Task.FromResult(new Mock<IConnection>().Object);
        var result = original;
        RabbitMqConnectionPatch.Postfix(transport, ref result);
        Assert.Same(original, result);
    }

    [Fact]
    public async Task FailedConnectionDoesNotRequestCredentials()
    {
        var provider = Provider("token");
        var failure = new InvalidOperationException("connection failed");
        var actual = await Assert.ThrowsAsync<InvalidOperationException>(() => RabbitMqConnectionPatch.RegisterAsync(
            Task.FromException<IConnection>(failure), new RabbitMqTransport("rabbitmq"), provider.Object));
        Assert.Same(failure, actual);
        provider.Verify(x => x.GetCredentialsAsync(It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task EmptyTokenIsNotSentAndCancellationEndsRetryDelay()
    {
        var connection = new Mock<IConnection>();
        connection.SetupGet(x => x.IsOpen).Returns(true);
        var requested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var provider = new Mock<ICredentialsProvider>();
        provider.Setup(x => x.GetCredentialsAsync(It.IsAny<CancellationToken>())).Returns(() =>
        {
            requested.TrySetResult();
            return Task.FromResult(new Credentials("oauth", "user", "", TimeSpan.FromMinutes(1)));
        });
        var refresher = new RabbitMqTokenRefresher(connection.Object, provider.Object, NullLogger.Instance);
        refresher.Start();
        await requested.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await refresher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        connection.Verify(x => x.UpdateSecretAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    private static Mock<ICredentialsProvider> Provider(string token)
    {
        var provider = new Mock<ICredentialsProvider>();
        provider.Setup(x => x.GetCredentialsAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new Credentials("oauth", "user", token, TimeSpan.FromMinutes(1)));
        return provider;
    }
}
