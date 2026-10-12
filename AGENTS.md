# Repository instructions

This repository provides an automatic startup patch package for WolverineFx.RabbitMQ 6.49.0 on .NET 9/10. The runtime uses Harmony; the packaged source generator creates a ModuleInitializer in the consuming C# application.

## Development

- Use the SDK selected by global.json. In the cloud environment, prepend /workspace/.dotnet to PATH and set DOTNET_CLI_HOME=/workspace/.dotnet-home and NUGET_PACKAGES=/workspace/.nuget/packages.
- Use the existing checkout. Cloud tasks are isolated; do not create a Git worktree unless the user requests one.
- Shared compiler settings belong in Directory.Build.props; dependency versions belong in Directory.Packages.props. Keep PackageReference versions out of repository project files.
- Keep WolverineFx.RabbitMQ pinned to 6.49.0 until a compatibility change is requested and verified.
- Consumers must only need a normal PackageReference in their executable C# project. Do not require EmitCompilerGeneratedFiles, manual registration, or custom MSBuild imports for normal use.
- Preserve the analyzer DLL under analyzers/dotnet/cs in the NuGet package. It must not become a runtime dependency.
- Never log token values or introduce credentials into source files.

## Validation

Run from the repository root:

```sh
dotnet build Osk.WolverineFx.RabbitMq.OAuth2.Patch.slnx -c Release
dotnet run --project samples/StartupSmoke -c Release
dotnet test --project tests/Osk.WolverineFx.RabbitMq.OAuth2.Patch.Tests -c Release
dotnet pack src/Osk.WolverineFx.RabbitMq.OAuth2.Patch -c Release -o artifacts
bash tools/verify-package-consumer.sh
```

Tests use TUnit and the Microsoft Testing Platform runner selected in global.json. Preserve the `--project` option for .NET 10 MTP commands; do not add xUnit or VSTest adapters. Process-wide Harmony registration tests must remain nonparallel.

The in-repository smoke example references the runtime project and the generator project (OutputItemType=Analyzer, ReferenceOutputAssembly=false). Keep solution builds independent of this unpublished package. The separate consumer verification script must retain its PackageReference to validate the actual NuGet artifact.

The verification script builds a consumer outside this checkout with an isolated NuGet cache stored outside its source directory. It must print `PASS: NuGet generator installed both Harmony patches before Main.` without calling Install explicitly or inheriting repository props files.

Compilation covers net9.0 and net10.0; unit tests and the smoke consumer run on net10.0. Unit tests use mocked connections. Do not claim real RabbitMQ/OAuth2 integration is verified without running it against configured services. Do not publish to NuGet.org unless the user requests publication.
