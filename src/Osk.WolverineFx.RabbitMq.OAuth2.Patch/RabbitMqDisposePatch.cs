using HarmonyLib;
using Wolverine.RabbitMQ.Internal;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Patch;

[HarmonyPatch(typeof(RabbitMqTransport), nameof(RabbitMqTransport.DisposeAsync))]
internal static class RabbitMqDisposePatch
{
    [HarmonyPrefix]
    public static void Prefix(RabbitMqTransport __instance) => RabbitMqRefreshRegistry.TryCancel(__instance);

    [HarmonyPostfix]
    public static void Postfix(RabbitMqTransport __instance, ref ValueTask __result) =>
        __result = DisposeAsync(__result, __instance);

    private static async ValueTask DisposeAsync(ValueTask original, RabbitMqTransport transport)
    {
        try { await original.ConfigureAwait(false); }
        finally { await RabbitMqRefreshRegistry.TryDisposeAsync(transport).ConfigureAwait(false); }
    }
}
