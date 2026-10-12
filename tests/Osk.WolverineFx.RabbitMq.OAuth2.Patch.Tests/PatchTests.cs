using HarmonyLib;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Wolverine.RabbitMQ.Internal;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Patch.Tests;

// Harmony changes process-wide method state, so these tests run sequentially.
[NotInParallel]
public class PatchTests
{
    [Test]
    public async Task InstallsBothPatchesExactlyOnce()
    {
        RabbitMqOAuth2Patches.Install();
        RabbitMqOAuth2Patches.Install();
        var create = AccessTools.DeclaredMethod(typeof(RabbitMqTransport), "CreateConnectionAsync");
        var dispose = AccessTools.DeclaredMethod(typeof(RabbitMqTransport), nameof(RabbitMqTransport.DisposeAsync));
        await Assert.That(Harmony.GetPatchInfo(create)!.Postfixes.Count(p => p.owner == RabbitMqOAuth2Patches.HarmonyId)).IsEqualTo(1);
        await Assert.That(Harmony.GetPatchInfo(dispose)!.Prefixes.Count(p => p.owner == RabbitMqOAuth2Patches.HarmonyId)).IsEqualTo(1);
        await Assert.That(Harmony.GetPatchInfo(dispose)!.Postfixes.Count(p => p.owner == RabbitMqOAuth2Patches.HarmonyId)).IsEqualTo(1);
    }

    [Test]
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
        await Assert.That(result).IsSameReferenceAs(connection.Object);
        await Assert.That(await updated.Task.WaitAsync(TimeSpan.FromSeconds(10))).IsEqualTo("new-token");
        await transport.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        provider.Verify(x => x.GetCredentialsAsync(It.IsAny<CancellationToken>()), Times.Once);
        connection.VerifyRemove(x => x.ConnectionShutdownAsync -= It.IsAny<RabbitMQ.Client.Events.AsyncEventHandler<ShutdownEventArgs>>(), Times.Once);
    }

    [Test]
    public async Task PasswordOnlyConnectionIsNotWrapped()
    {
        var transport = new RabbitMqTransport("rabbitmq");
        typeof(RabbitMqTransport).GetProperty(nameof(RabbitMqTransport.ConnectionFactory))!
            .SetValue(transport, new ConnectionFactory { CredentialsProvider = new BasicCredentialsProvider("basic", "user", "password") });
        var original = Task.FromResult(new Mock<IConnection>().Object);
        var result = original;
        RabbitMqConnectionPatch.Postfix(transport, ref result);
        await Assert.That((object)result).IsSameReferenceAs(original);
    }

    [Test]
    public async Task FailedConnectionDoesNotRequestCredentials()
    {
        var provider = Provider("token");
        var failure = new InvalidOperationException("connection failed");
        var actual = await Assert.That(async () =>
            await RabbitMqConnectionPatch.RegisterAsync(
                Task.FromException<IConnection>(failure), new RabbitMqTransport("rabbitmq"), provider.Object))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(actual).IsSameReferenceAs(failure);
        provider.Verify(x => x.GetCredentialsAsync(It.IsAny<CancellationToken>()), Times.Never);
    }

    [Test]
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
