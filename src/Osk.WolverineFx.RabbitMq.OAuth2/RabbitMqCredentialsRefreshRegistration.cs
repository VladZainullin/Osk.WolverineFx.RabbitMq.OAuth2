using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.OAuth2;

namespace Osk.WolverineFx.RabbitMq.OAuth2;

/// <summary>Связывает штатный планировщик обновления учётных данных с соединением и временем жизни транспорта.</summary>
/// <remarks>Владеет <see cref="CredentialsRefresher"/> и источником отмены, но не соединением и не поставщиком учётных данных. Планирование выполняет только штатный refresher. Закрытие соединения приложением отменяет работу; полное освобождение требует <see cref="DisposeAsync"/>.</remarks>
internal sealed class RabbitMqCredentialsRefreshRegistration : IAsyncDisposable
{
    /// <summary>Соединение, которому передаются обновлённые токены; регистрация его не освобождает.</summary>
    private readonly IConnection _connection;
    /// <summary>Журнал ошибок получения и применения токенов без записи их значений.</summary>
    private readonly ILogger _logger;
    /// <summary>Синхронизирует отмену и запуск единственного освобождения ресурсов.</summary>
    private readonly Lock _sync = new();
    /// <summary>Отменяет запросы учётных данных, задержки планировщика и обновление секрета.</summary>
    private readonly CancellationTokenSource _cts = new();
    /// <summary>Штатный планировщик RabbitMQ.Client.OAuth2, освобождаемый вместе с регистрацией.</summary>
    private readonly CredentialsRefresher _refresher;
    /// <summary>Общая задача освобождения для всех повторных и параллельных вызовов.</summary>
    private Task? _disposeTask;

    /// <summary>Подписывается на закрытие соединения и запускает штатное обновление учётных данных.</summary>
    /// <param name="connection">Соединение для передачи новых токенов.</param>
    /// <param name="provider">Поставщик токенов и сроков их действия.</param>
    /// <param name="logger">Журнал диагностики обновления.</param>
    /// <remarks>При ошибке создания планировщика снимает подписку, отменяет работу и освобождает источник отмены перед передачей ошибки вызывающему коду.</remarks>
    public RabbitMqCredentialsRefreshRegistration(IConnection connection, ICredentialsProvider provider, ILogger logger)
    {
        _connection = connection;
        _logger = logger;
        _connection.ConnectionShutdownAsync += OnShutdownAsync;
        try
        {
            _refresher = new CredentialsRefresher(provider, OnRefreshedAsync, _cts.Token);
        }
        catch
        {
            lock (_sync)
            {
                try { Cancel(); }
                finally
                {
                    _disposeTask = Task.CompletedTask;
                    _cts.Dispose();
                }
            }
            throw;
        }
    }

    /// <summary>Отменяет обновление при закрытии соединения приложением.</summary>
    /// <param name="sender">Источник события закрытия.</param>
    /// <param name="args">Причина закрытия и его инициатор.</param>
    /// <returns>Завершённая задача обработки события.</returns>
    /// <remarks>Закрытие брокером или библиотекой не отменяет планировщик, чтобы сохранить обновление после восстановления соединения.</remarks>
    private Task OnShutdownAsync(object sender, ShutdownEventArgs args)
    {
        if (args.Initiator == ShutdownInitiator.Application) Cancel();
        return Task.CompletedTask;
    }

    /// <summary>Передаёт полученный токен открытому соединению либо регистрирует ошибку обновления.</summary>
    /// <param name="credentials">Полученные учётные данные; могут отсутствовать при ошибке поставщика.</param>
    /// <param name="exception">Ошибка получения учётных данных, переданная планировщиком.</param>
    /// <param name="cancellationToken">Токен остановки планировщика и обновления секрета соединения.</param>
    /// <returns>Задача обработки результата обновления.</returns>
    /// <remarks>Пустой токен и закрытое соединение исключают отправку секрета. Ошибки поставщика и отправки журналируются; ожидаемая отмена отправки подавляется. При уже запрошенной отмене обработка не начинается.</remarks>
    private async Task OnRefreshedAsync(Credentials? credentials, Exception? exception, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested) return;
        if (exception is not null)
        {
            _logger.LogError(exception, "RabbitMQ OAuth2 credentials refresh failed");
            return;
        }

        if (credentials is null || string.IsNullOrWhiteSpace(credentials.Password))
        {
            _logger.LogError("RabbitMQ OAuth2 provider returned an empty token");
            return;
        }

        if (!_connection.IsOpen) return;
        try
        {
            await _connection.UpdateSecretAsync(credentials.Password, "OAuth2 access token refresh", cancellationToken)
                .ConfigureAwait(false);
            _logger.LogDebug("RabbitMQ OAuth2 token refreshed successfully");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "RabbitMQ OAuth2 connection token update failed");
        }
    }

    /// <summary>Снимает подписку на закрытие соединения и запрашивает остановку планировщика.</summary>
    /// <remarks>Повторные вызовы безопасны. Не ожидает завершения работы и не освобождает планировщик; для этого нужен <see cref="DisposeAsync"/>. Уже освобождённое соединение, запрещающее отписку, не препятствует отмене.</remarks>
    public void Cancel()
    {
        lock (_sync)
        {
            if (_disposeTask is not null || _cts.IsCancellationRequested) return;
            // The transport prefix calls this while the connection still permits event access.
            // An externally disposed AutorecoveringConnection rejects event unsubscription.
            try { _connection.ConnectionShutdownAsync -= OnShutdownAsync; }
            catch (ObjectDisposedException) { }
            _cts.Cancel();
        }
    }

    /// <summary>Отменяет обновление и ожидает завершения планировщика с освобождением его ресурсов.</summary>
    /// <returns>Операция ожидания общей задачи освобождения регистрации.</returns>
    /// <remarks>Повторные и параллельные вызовы ожидают одну задачу. Синхронный <see cref="CredentialsRefresher.Dispose()"/> выполняется в пуле потоков; источник отмены освобождается после него. Соединение и поставщик учётных данных остаются во владении вызывающего кода.</remarks>
    public ValueTask DisposeAsync()
    {
        lock (_sync)
        {
            if (_disposeTask is not null) return new ValueTask(_disposeTask);
            Cancel();
            _disposeTask = Task.Run(DisposeRefresher);
            return new ValueTask(_disposeTask);
        }
    }

    /// <summary>Завершает штатный планировщик и в любом случае освобождает источник отмены.</summary>
    /// <remarks>Для RabbitMQ.Client.OAuth2 2.0.0 подавляет только агрегат исключений отмены при запрошенной остановке. Остальные ошибки сохраняются в задаче освобождения.</remarks>
    private void DisposeRefresher()
    {
        try { _refresher.Dispose(); }
        // OAuth2 2.0.0 throws an aggregate if cancellation interrupts its delay.
        catch (AggregateException ex) when (_cts.IsCancellationRequested &&
            ex.Flatten().InnerExceptions.All(e => e is OperationCanceledException))
        { }
        finally
        {
            _cts.Dispose();
        }
    }
}
