using HarmonyLib;

namespace Osk.WolverineFx.RabbitMq.OAuth2;

/// <summary>Устанавливает Harmony-патчи создания соединений и освобождения транспорта Wolverine.</summary>
/// <remarks>Патчи действуют во всём процессе. Генератор вызывает установку автоматически при инициализации модуля потребителя.</remarks>
/// <seealso cref="RabbitMqTransportCreateConnectionAsyncPatch"/>
/// <seealso cref="RabbitMqTransportDisposeAsyncPatch"/>
public static class RabbitMqOAuth2Patches
{
    /// <summary>Идентификатор владельца Harmony-патчей для их поиска и отката.</summary>
    public const string HarmonyId = "Osk.WolverineFx.RabbitMq.OAuth2";
    /// <summary>Синхронизирует установку патчей во всём процессе.</summary>
    private static readonly Lock Sync = new();
    /// <summary>Показывает, что оба патча успешно установлены.</summary>
    private static bool _installed;

    /// <summary>Однократно устанавливает оба патча, сохраняя безопасность повторных и параллельных вызовов.</summary>
    /// <remarks>При ошибке установки удаляет патчи с владельцем <see cref="HarmonyId"/> и передаёт ошибку вызывающему коду; последующий вызов может повторить установку.</remarks>
    public static void Install()
    {
        lock (Sync)
        {
            if (_installed) return;
            var harmony = new Harmony(HarmonyId);
            try
            {
                harmony.CreateClassProcessor(typeof(RabbitMqTransportCreateConnectionAsyncPatch)).Patch();
                harmony.CreateClassProcessor(typeof(RabbitMqTransportDisposeAsyncPatch)).Patch();
                _installed = true;
            }
            catch
            {
                harmony.UnpatchAll(HarmonyId);
                throw;
            }
        }
    }
}
