using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Wolverine.RabbitMQ.Internal;

namespace Osk.WolverineFx.RabbitMq.OAuth2;

/// <summary>Хранит регистрации обновления токенов для одного транспорта Wolverine.</summary>
/// <param name="logger">Журнал, передаваемый регистрациям соединений.</param>
/// <remarks>Слабый ключ транспорта позволяет сборщику мусора удалить реестр. Остановленный реестр сохраняет запрет на поздние регистрации до удаления самого транспорта.</remarks>
internal sealed class RabbitMqRefreshRegistry(ILogger logger)
{
    /// <summary>Связывает транспорт с реестром, не удерживая транспорт от сборки мусора.</summary>
    private static readonly ConditionalWeakTable<RabbitMqTransport, RabbitMqRefreshRegistry> Registries = new();
    /// <summary>Синхронизирует регистрацию соединений и остановку реестра.</summary>
    private readonly Lock _sync = new();
    /// <summary>Регистрации, ресурсы которых должен освободить транспорт.</summary>
    private readonly List<RabbitMqCredentialsRefreshRegistration> _refreshers = [];
    /// <summary>Общая задача завершения освобождения всех регистраций.</summary>
    private Task? _disposeTask;
    /// <summary>Запрещает регистрацию соединений после начала остановки транспорта.</summary>
    private bool _stopping;

    /// <summary>Возвращает единственный реестр транспорта, создавая его при первом обращении.</summary>
    /// <param name="transport">Транспорт, которому принадлежат соединения.</param>
    /// <returns>Реестр, связанный с транспортом.</returns>
    public static RabbitMqRefreshRegistry For(RabbitMqTransport transport) =>
        Registries.GetValue(transport, static t => new RabbitMqRefreshRegistry(t.Logger));

    /// <summary>Запускает обновление токенов соединения и передаёт регистрацию во владение реестра.</summary>
    /// <param name="connection">Созданное соединение для обновления секрета.</param>
    /// <param name="provider">Поставщик обновляемых учётных данных.</param>
    /// <exception cref="ObjectDisposedException">Остановка реестра уже началась.</exception>
    public void Register(IConnection connection, ICredentialsProvider provider)
    {
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_stopping, this);
            _refreshers.Add(new RabbitMqCredentialsRefreshRegistration(connection, provider, logger));
        }
    }

    /// <summary>Запрещает новые регистрации и отменяет обновление токенов перед закрытием соединений транспорта.</summary>
    /// <param name="transport">Останавливаемый транспорт.</param>
    /// <remarks>Создаёт остановленный реестр, даже если соединений ещё нет, чтобы позднее создание соединения не запустило обновление.</remarks>
    public static void TryCancel(RabbitMqTransport transport)
    {
        var registry = For(transport);
        lock (registry._sync)
        {
            registry._stopping = true;
            foreach (var refresher in registry._refreshers) refresher.Cancel();
        }
    }

    /// <summary>Ожидает освобождения регистраций, если реестр транспорта существует.</summary>
    /// <param name="transport">Транспорт, реестр которого нужно освободить.</param>
    /// <returns>Операция освобождения реестра либо завершённая операция, если реестр отсутствует.</returns>
    /// <remarks>Остановленный реестр сохраняется под слабым ключом транспорта для отклонения поздних регистраций. Собственного токена отмены ожидание не принимает.</remarks>
    public static ValueTask TryDisposeAsync(RabbitMqTransport transport)
    {
        // Keep a stopped entry so a late connection cannot restart refresh after shutdown.
        // The weak transport key still permits collection of the registry.
        return Registries.TryGetValue(transport, out var registry)
            ? new ValueTask(registry.DisposeAsync())
            : ValueTask.CompletedTask;
    }

    /// <summary>Запрещает новые регистрации и однократно запускает освобождение всех накопленных регистраций.</summary>
    /// <returns>Общая задача ожидания всех регистраций, сохраняемая для повторных вызовов.</returns>
    private Task DisposeAsync()
    {
        lock (_sync)
        {
            if (_disposeTask is not null) return _disposeTask;
            _stopping = true;
            var refreshers = _refreshers.ToArray();
            _refreshers.Clear();
            _disposeTask = Task.WhenAll(refreshers.Select(static x => x.DisposeAsync().AsTask()));
            return _disposeTask;
        }
    }
}
