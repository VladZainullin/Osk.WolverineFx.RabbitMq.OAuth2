using HarmonyLib;
using Wolverine.RabbitMQ.Internal;

namespace Osk.WolverineFx.RabbitMq.OAuth2;

/// <summary>Связывает освобождение транспорта Wolverine с остановкой и освобождением обновления OAuth2-токенов.</summary>
/// <remarks>Prefix отменяет обновление до закрытия соединений, пока доступна отписка от событий. Postfix дополняет исходное освобождение ожиданием завершения планировщиков даже при ошибке Wolverine.</remarks>
[HarmonyPatch(typeof(RabbitMqTransport), nameof(RabbitMqTransport.DisposeAsync))]
internal static class RabbitMqTransportDisposeAsyncPatch
{
    /// <summary>Отменяет обновление токенов до выполнения DisposeAsync транспорта Wolverine.</summary>
    /// <param name="transport">Освобождаемый транспорт.</param>
    /// <remarks>Создаёт остановленный реестр даже для транспорта без соединений, чтобы отклонять поздние регистрации.</remarks>
    [HarmonyPrefix]
    public static void Prefix([HarmonyArgument("__instance")] RabbitMqTransport transport) => RabbitMqRefreshRegistryStore.GetOrCreate(transport).Cancel();

    /// <summary>Дополняет результат DisposeAsync транспорта ожиданием освобождения регистраций обновления.</summary>
    /// <param name="transport">Освобождаемый транспорт.</param>
    /// <param name="result">Исходная операция Wolverine, заменяемая обёрткой освобождения.</param>
    [HarmonyPostfix]
    public static void Postfix(
        [HarmonyArgument("__instance")] RabbitMqTransport transport,
        [HarmonyArgument("__result")] ref ValueTask result) =>
        result = DisposeAsync(result, RabbitMqRefreshRegistryStore.GetOrCreate(transport));

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
