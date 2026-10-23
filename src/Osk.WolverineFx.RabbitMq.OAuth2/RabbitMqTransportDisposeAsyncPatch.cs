using HarmonyLib;
using Wolverine.RabbitMQ.Internal;

namespace Osk.WolverineFx.RabbitMq.OAuth2;

/// <summary>Связывает освобождение транспорта Wolverine с остановкой и освобождением обновления OAuth2-токенов.</summary>
/// <remarks>Prefix отменяет обновление до закрытия соединений, пока доступна отписка от событий. Postfix дополняет исходное освобождение ожиданием завершения планировщиков даже при ошибке Wolverine.</remarks>
[HarmonyPatch(typeof(RabbitMqTransport), nameof(RabbitMqTransport.DisposeAsync))]
internal static class RabbitMqTransportDisposeAsyncPatch
{
    /// <summary>Отменяет обновление токенов до выполнения DisposeAsync транспорта Wolverine.</summary>
    /// <param name="__instance">Освобождаемый транспорт.</param>
    [HarmonyPrefix]
    public static void Prefix(RabbitMqTransport __instance) => RabbitMqRefreshRegistry.TryCancel(__instance);

    /// <summary>Дополняет результат DisposeAsync транспорта ожиданием освобождения регистраций обновления.</summary>
    /// <param name="__instance">Освобождаемый транспорт.</param>
    /// <param name="__result">Исходная операция Wolverine, заменяемая обёрткой освобождения.</param>
    [HarmonyPostfix]
    public static void Postfix(RabbitMqTransport __instance, ref ValueTask __result) =>
        __result = DisposeAsync(__result, __instance);

    /// <summary>Ожидает освобождения транспорта и гарантированно запускает освобождение его реестра.</summary>
    /// <param name="original">Исходная операция освобождения транспорта.</param>
    /// <param name="transport">Транспорт, которому принадлежат регистрации.</param>
    /// <returns>Операция, завершающаяся после транспорта и его планировщиков обновления.</returns>
    /// <remarks>Освобождение реестра выполняется в finally. Ошибки не подавляются; ошибка реестра может заменить ошибку исходной операции. Ожидание не отменяется отдельным токеном, чтобы завершить очистку ресурсов.</remarks>
    private static async ValueTask DisposeAsync(ValueTask original, RabbitMqTransport transport)
    {
        try { await original.ConfigureAwait(false); }
        finally { await RabbitMqRefreshRegistry.TryDisposeAsync(transport).ConfigureAwait(false); }
    }
}
