using HarmonyLib;
using RabbitMQ.Client;
using Wolverine.RabbitMQ.Internal;

namespace Osk.WolverineFx.RabbitMq.OAuth2;

/// <summary>Перехватывает завершение создания соединения RabbitMqTransport для запуска обновления OAuth2-токенов.</summary>
/// <remarks>Postfix оборачивает задачу Wolverine, сохраняя результат создания соединения. Соединения без поставщика учётных данных и с <see cref="BasicCredentialsProvider"/> не затрагиваются.</remarks>
[HarmonyPatch(typeof(RabbitMqTransport), "CreateConnectionAsync")]
internal static class RabbitMqTransportCreateConnectionAsyncPatch
{
    /// <summary>Заменяет задачу создания OAuth2-соединения задачей, которая также регистрирует обновление токенов.</summary>
    /// <param name="__instance">Транспорт Wolverine, создающий соединение.</param>
    /// <param name="__result">Исходная задача; для OAuth2 заменяется обёрткой регистрации.</param>
    [HarmonyPostfix]
    internal static void Postfix(RabbitMqTransport __instance, ref Task<IConnection> __result)
    {
        // Password-only connections do not have expiring OAuth2 credentials.
        var provider = __instance.ConnectionFactory?.CredentialsProvider;
        if (provider is null or BasicCredentialsProvider) return;
        __result = RegisterAsync(__result, __instance, provider);
    }

    /// <summary>Дожидается создания соединения и регистрирует его в реестре транспорта.</summary>
    /// <param name="original">Исходная задача Wolverine по созданию соединения.</param>
    /// <param name="transport">Транспорт, определяющий время жизни обновления.</param>
    /// <param name="provider">Поставщик обновляемых токенов.</param>
    /// <returns>Задача, возвращающая исходное соединение после успешной регистрации.</returns>
    /// <exception cref="ObjectDisposedException">Реестр транспорта уже останавливается и отклоняет регистрацию.</exception>
    /// <remarks>Ошибка или отмена исходной задачи передаётся вызывающему коду без регистрации. При ошибке регистрации созданное соединение освобождается, после чего ошибка передаётся вызывающему коду.</remarks>
    internal static async Task<IConnection> RegisterAsync(
        Task<IConnection> original, RabbitMqTransport transport, ICredentialsProvider provider)
    {
        var connection = await original.ConfigureAwait(false);
        try
        {
            RabbitMqRefreshRegistryStore.GetOrCreate(transport).Register(connection, provider);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}
