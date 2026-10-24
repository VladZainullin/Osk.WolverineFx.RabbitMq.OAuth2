using System.Runtime.CompilerServices;
using Wolverine.RabbitMQ.Internal;

namespace Osk.WolverineFx.RabbitMq.OAuth2;

/// <summary>Хранит и возвращает реестры обновления токенов, связанные с транспортами Wolverine.</summary>
/// <remarks>Слабый ключ не препятствует сборке мусора транспорта. Остановленный реестр сохраняется до удаления транспорта для отклонения поздних регистраций.</remarks>
internal static class RabbitMqRefreshRegistryStore
{
    private static readonly ConditionalWeakTable<RabbitMqTransport, RabbitMqRefreshRegistry> Registries = new();

    /// <summary>Возвращает единственный реестр транспорта, создавая его при первом обращении.</summary>
    /// <param name="transport">Транспорт, которому принадлежат соединения.</param>
    /// <returns>Реестр, связанный с транспортом.</returns>
    public static RabbitMqRefreshRegistry GetOrCreate(RabbitMqTransport transport) =>
        Registries.GetValue(transport, Create);

    private static RabbitMqRefreshRegistry Create(RabbitMqTransport transport)
    {
        return new RabbitMqRefreshRegistry(transport.ConnectionFactory?.CredentialsProvider, transport.Logger);
    }
}
