using HarmonyLib;
using RabbitMQ.Client;
using Wolverine.RabbitMQ.Internal;

namespace Osk.WolverineFx.RabbitMq.OAuth2;

[HarmonyPatch(typeof(RabbitMqTransport), "CreateConnectionAsync")]
internal static class RabbitMqTransportCreateConnectionAsyncPatch
{
    [HarmonyPostfix]
    public static void Postfix([HarmonyArgument("__instance")] RabbitMqTransport transport,
        [HarmonyArgument("__result")] ref Task<IConnection> result)
    {
        var provider = transport.ConnectionFactory?.CredentialsProvider;
        if (provider is null or BasicCredentialsProvider) return;
        result = RegisterAsync(result, transport);
    }

    public static async Task<IConnection> RegisterAsync(Task<IConnection> original, RabbitMqTransport transport)
    {
        var connection = await original.ConfigureAwait(false);
        try
        {
            RabbitMqRefreshRegistryStore.GetOrCreate(transport).Register(connection);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}