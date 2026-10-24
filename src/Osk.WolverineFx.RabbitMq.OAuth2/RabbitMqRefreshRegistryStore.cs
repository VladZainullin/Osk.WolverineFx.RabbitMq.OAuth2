using System.Runtime.CompilerServices;
using Wolverine.RabbitMQ.Internal;

namespace Osk.WolverineFx.RabbitMq.OAuth2;

/// <summary>Хранит и возвращает реестры обновления токенов, связанные с транспортами Wolverine.</summary>
/// <remarks>Слабый ключ не препятствует сборке мусора транспорта. Остановленный реестр сохраняется до удаления транспорта для отклонения поздних регистраций.</remarks>
internal static class RabbitMqRefreshRegistryStore
{
    /// <summary>Потокобезопасно сопоставляет каждому транспорту единственный реестр, не удерживая транспорт от сборки мусора.</summary>
    private static readonly ConditionalWeakTable<RabbitMqTransport, RabbitMqRefreshRegistry> Registries = new();

    /// <summary>Возвращает единственный реестр транспорта, создавая его при первом обращении.</summary>
    /// <param name="transport">Транспорт, которому принадлежат соединения.</param>
    /// <returns>Реестр, связанный с транспортом.</returns>
    public static RabbitMqRefreshRegistry GetOrCreate(RabbitMqTransport transport) =>
        Registries.GetValue(transport, static transport => new RabbitMqRefreshRegistry(transport.Logger));
}
