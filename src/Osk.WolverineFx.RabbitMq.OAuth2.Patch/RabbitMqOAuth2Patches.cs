using HarmonyLib;
using Wolverine.RabbitMQ.Internal;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Patch;

/// <summary>Installs the two process-wide Harmony patches. Repeated calls are safe.</summary>
public static class RabbitMqOAuth2Patches
{
    public const string HarmonyId = "Osk.WolverineFx.RabbitMq.OAuth2.Patch";
    private static readonly Lock Sync = new();
    private static bool _installed;

    public static void Install()
    {
        lock (Sync)
        {
            if (_installed) return;
            var harmony = new Harmony(HarmonyId);
            try
            {
                harmony.CreateClassProcessor(typeof(RabbitMqConnectionPatch)).Patch();
                harmony.CreateClassProcessor(typeof(RabbitMqDisposePatch)).Patch();
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
