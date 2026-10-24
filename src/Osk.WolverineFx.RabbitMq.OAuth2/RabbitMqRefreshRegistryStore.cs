using System.Runtime.CompilerServices;
using Wolverine.RabbitMQ.Internal;

namespace Osk.WolverineFx.RabbitMq.OAuth2;

/// <summary>Хранит и возвращает реестры обновления токенов, связанные с транспортами Wolverine.</summary>
/// <remarks>Слабый ключ не препятствует сборке мусора транспорта. Остановленный реестр сохраняется до удаления транспорта для отклонения поздних регистраций.</remarks>
internal static class RabbitMqRefreshRegistryStore
{
    private static readonly ConditionalWeakTable<RabbitMqTransport, RabbitMqTransportConnectionsRegistry> Registries = new();

    /// <summary>Возвращает единственный реестр транспорта, создавая его при первом обращении.</summary>
    /// <param name="transport">Транспорт, которому принадлежат соединения.</param>
    /// <returns>Реестр, связанный с транспортом.</returns>
    public static RabbitMqTransportConnectionsRegistry GetOrCreate(RabbitMqTransport transport) =>
        Registries.GetValue(transport, Create);

    private static RabbitMqTransportConnectionsRegistry Create(RabbitMqTransport transport)
    {
        return new RabbitMqTransportConnectionsRegistry(transport.ConnectionFactory?.CredentialsProvider, transport.Logger);
    }
}
