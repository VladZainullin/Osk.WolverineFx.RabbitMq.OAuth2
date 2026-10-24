using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Osk.WolverineFx.RabbitMq.OAuth2;

/// <summary>Хранит регистрации обновления токенов для одного транспорта Wolverine.</summary>
/// <param name="logger">Журнал, передаваемый регистрациям соединений.</param>
/// <remarks>После начала остановки отклоняет поздние регистрации и однократно освобождает накопленные ресурсы.</remarks>
internal sealed class RabbitMqRefreshRegistry(ILogger logger)
{
    /// <summary>Синхронизирует регистрацию соединений и остановку реестра.</summary>
    private readonly Lock _sync = new();
    /// <summary>Регистрации, ресурсы которых должен освободить транспорт.</summary>
    private readonly List<RabbitMqCredentialsRefreshRegistration> _refreshers = [];
    /// <summary>Общая задача завершения освобождения всех регистраций.</summary>
    private Task? _disposeTask;
    /// <summary>Запрещает регистрацию соединений после начала остановки транспорта.</summary>
    private bool _stopping;

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
    /// <remarks>Сначала запрещает новые регистрации, в том числе при отсутствии соединений. Повторные вызовы безопасны.</remarks>
    public void Cancel()
    {
        lock (_sync)
        {
            _stopping = true;
            foreach (var refresher in _refreshers) refresher.Cancel();
        }
    }

    /// <summary>Запрещает новые регистрации и однократно запускает освобождение всех накопленных регистраций.</summary>
    /// <returns>Общая задача ожидания всех регистраций, сохраняемая для повторных вызовов.</returns>
    public Task DisposeAsync()
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
