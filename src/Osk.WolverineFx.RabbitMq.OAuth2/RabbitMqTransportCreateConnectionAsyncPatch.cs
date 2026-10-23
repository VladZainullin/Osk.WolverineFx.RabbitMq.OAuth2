using HarmonyLib;
using RabbitMQ.Client;
using Wolverine.RabbitMQ.Internal;

namespace Osk.WolverineFx.RabbitMq.OAuth2;

[HarmonyPatch(typeof(RabbitMqTransport), "CreateConnectionAsync")]
internal static class RabbitMqTransportCreateConnectionAsyncPatch
{
    [HarmonyPostfix]
    internal static void Postfix(RabbitMqTransport __instance, ref Task<IConnection> __result)
    {
        // Password-only connections do not have expiring OAuth2 credentials.
        var provider = __instance.ConnectionFactory?.CredentialsProvider;
        if (provider is null || provider is BasicCredentialsProvider) return;
        __result = RegisterAsync(__result, __instance, provider);
    }

    internal static async Task<IConnection> RegisterAsync(
        Task<IConnection> original, RabbitMqTransport transport, ICredentialsProvider provider)
    {
        var connection = await original.ConfigureAwait(false);
        try
        {
            RabbitMqRefreshRegistry.For(transport).Register(connection, provider);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}
