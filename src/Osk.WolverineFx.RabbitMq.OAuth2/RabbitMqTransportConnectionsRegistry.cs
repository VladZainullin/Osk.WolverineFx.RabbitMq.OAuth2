using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Osk.WolverineFx.RabbitMq.OAuth2;

/// <summary>Хранит регистрации обновления токенов для одного транспорта Wolverine.</summary>
/// <param name="provider">Общий поставщик учётных данных транспорта; может отсутствовать у транспорта без OAuth2-соединений.</param>
/// <param name="logger">Журнал, передаваемый регистрациям соединений.</param>
/// <remarks>После начала остановки отклоняет поздние регистрации и однократно освобождает накопленные ресурсы.</remarks>
internal sealed class RabbitMqTransportConnectionsRegistry(ICredentialsProvider? provider, ILogger logger)
{
    private readonly Lock _lock = new();

    private readonly List<ConnectionCredentialsRefresherRelation> _refreshers = [];

    private Task? _disposeTask;

    private bool _stopping;

    /// <summary>Запускает обновление токенов соединения и передаёт регистрацию во владение реестра.</summary>
    /// <param name="connection">Созданное соединение для обновления секрета.</param>
    /// <exception cref="ArgumentNullException">У транспорта отсутствует поставщик учётных данных.</exception>
    /// <exception cref="ObjectDisposedException">Остановка реестра уже началась.</exception>
    public void Register(IConnection connection)
    {
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_stopping, this);
            ArgumentNullException.ThrowIfNull(provider);
            _refreshers.Add(new ConnectionCredentialsRefresherRelation(connection, provider, logger));
        }
    }

    /// <summary>Запрещает новые регистрации и отменяет обновление токенов перед закрытием соединений транспорта.</summary>
    /// <remarks>Сначала запрещает новые регистрации, в том числе при отсутствии соединений. Повторные вызовы безопасны.</remarks>
    public void Cancel()
    {
        lock (_lock)
        {
            _stopping = true;
            foreach (var refresher in _refreshers) refresher.Cancel();
        }
    }

    /// <summary>Запрещает новые регистрации и однократно запускает освобождение всех накопленных регистраций.</summary>
    /// <returns>Общая задача ожидания всех регистраций, сохраняемая для повторных вызовов.</returns>
    public Task DisposeAsync()
    {
        lock (_lock)
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