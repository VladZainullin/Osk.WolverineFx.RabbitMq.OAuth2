using HarmonyLib;
using Osk.WolverineFx.RabbitMq.OAuth2;
using Wolverine.RabbitMQ.Internal;

// No explicit Install() call: the generator must install both patches before Main.
var create = AccessTools.DeclaredMethod(typeof(RabbitMqTransport), "CreateConnectionAsync");
var dispose = AccessTools.DeclaredMethod(typeof(RabbitMqTransport), nameof(RabbitMqTransport.DisposeAsync));
var createPatches = Harmony.GetPatchInfo(create);
var disposePatches = Harmony.GetPatchInfo(dispose);
if (createPatches?.Postfixes.Count(static p => p.owner == RabbitMqOAuth2Patches.HarmonyId) != 1 ||
    disposePatches?.Prefixes.Count(static p => p.owner == RabbitMqOAuth2Patches.HarmonyId) != 1 ||
    disposePatches?.Postfixes.Count(static p => p.owner == RabbitMqOAuth2Patches.HarmonyId) != 1)
    throw new InvalidOperationException("Automatic startup did not install both patches.");

Console.WriteLine("PASS: NuGet generator installed both Harmony patches before Main.");
