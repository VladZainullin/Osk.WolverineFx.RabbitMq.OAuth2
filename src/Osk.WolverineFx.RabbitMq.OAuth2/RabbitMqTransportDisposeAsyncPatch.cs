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
    /// <remarks>Создаёт остановленный реестр даже для транспорта без соединений, чтобы отклонять поздние регистрации.</remarks>
    [HarmonyPrefix]
    public static void Prefix(RabbitMqTransport __instance) => RabbitMqRefreshRegistryStore.GetOrCreate(__instance).Cancel();

    /// <summary>Дополняет результат DisposeAsync транспорта ожиданием освобождения регистраций обновления.</summary>
    /// <param name="__instance">Освобождаемый транспорт.</param>
    /// <param name="__result">Исходная операция Wolverine, заменяемая обёрткой освобождения.</param>
    [HarmonyPostfix]
    public static void Postfix(RabbitMqTransport __instance, ref ValueTask __result) =>
        __result = DisposeAsync(__result, RabbitMqRefreshRegistryStore.GetOrCreate(__instance));

    /// <summary>Ожидает освобождения транспорта и гарантированно запускает освобождение его реестра.</summary>
    /// <param name="original">Исходная операция освобождения транспорта.</param>
    /// <param name="registry">Реестр регистраций освобождаемого транспорта.</param>
    /// <returns>Операция, завершающаяся после транспорта и его планировщиков обновления.</returns>
    /// <remarks>Освобождение реестра выполняется в finally. Ошибки не подавляются; ошибка реестра может заменить ошибку исходной операции. Ожидание не отменяется отдельным токеном, чтобы завершить очистку ресурсов.</remarks>
    private static async ValueTask DisposeAsync(ValueTask original, RabbitMqRefreshRegistry registry)
    {
        try { await original.ConfigureAwait(false); }
        finally { await registry.DisposeAsync().ConfigureAwait(false); }
    }
}
