using HarmonyLib;
using Wolverine.RabbitMQ.Internal;

namespace Osk.WolverineFx.RabbitMq.OAuth2;

[HarmonyPatch(typeof(RabbitMqTransport), nameof(RabbitMqTransport.DisposeAsync))]
internal static class RabbitMqTransportDisposeAsyncPatch
{
    [HarmonyPrefix]
    public static void Prefix([HarmonyArgument("__instance")] RabbitMqTransport transport) =>
        RabbitMqRefreshRegistryStore.GetOrCreate(transport).Cancel();

    [HarmonyPostfix]
    public static void Postfix(
        [HarmonyArgument("__instance")] RabbitMqTransport transport,
        [HarmonyArgument("__result")] ref ValueTask result)
    {
        var registry = RabbitMqRefreshRegistryStore.GetOrCreate(transport);
        result = DisposeAsync(result, registry);
    }

    private static async ValueTask DisposeAsync(ValueTask original, RabbitMqTransportConnectionsRegistry registry)
    {
        try
        {
            await original.ConfigureAwait(false);
        }
        finally
        {
            await registry.DisposeAsync().ConfigureAwait(false);
        }
    }
}