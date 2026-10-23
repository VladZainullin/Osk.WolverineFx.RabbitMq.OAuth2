# Repository instructions

This repository provides an automatic startup patch package for WolverineFx.RabbitMQ 6.49.0 on .NET 9/10. The runtime uses Harmony; the packaged source generator creates a ModuleInitializer in the consuming C# application.

## Development

- Use the SDK selected by global.json. In the cloud environment, prepend /workspace/.dotnet to PATH and set DOTNET_CLI_HOME=/workspace/.dotnet-home and NUGET_PACKAGES=/workspace/.nuget/packages.
- Use the existing checkout. Cloud tasks are isolated; do not create a Git worktree unless the user requests one.
- Shared compiler settings belong in Directory.Build.props; dependency versions belong in Directory.Packages.props. Keep PackageReference versions out of repository project files.
- Keep WolverineFx.RabbitMQ pinned to 6.49.0 until a compatibility change is requested and verified.
- Name Harmony patch classes and their files using the target class name + target method name + Patch pattern (for example, RabbitMqTransportDisposeAsyncPatch).
- Consumers must only need a normal PackageReference in their executable C# project. Do not require EmitCompilerGeneratedFiles, manual registration, or custom MSBuild imports for normal use.
- Preserve the analyzer DLL under analyzers/dotnet/cs in the NuGet package. It must not become a runtime dependency.
- Name CancellationToken parameters and locals `cancellationToken`; do not use `ct`, `token`, or other ambiguous abbreviations. Use meaningful specialized names such as `shutdownCancellationToken` when multiple tokens are in scope.
- Put CancellationToken last in method, constructor, and delegate parameter lists wherever compatibility permits. Preserve required interface, override, and third-party API signatures; update call sites and XML parameter documentation when renaming or reordering. Named arguments such as `cancellationToken: cancellationToken` are welcome for readability. Apply these rules to runtime code, source generators, samples, tests, and test infrastructure without changing cancellation behavior, CancellationTokenSource lifetimes, or OperationCanceledException handling.
- Never log token values or introduce credentials into source files.
- Use RabbitMQ.Client.OAuth2 CredentialsRefresher for refresh scheduling. The registration adapter only sends credentials to the connection and manages shutdown; do not introduce a second timer or refresh loop.

## XML documentation

- Every new public or internal type and member must have meaningful XML documentation. Document private methods with complex logic as well.
- Update the corresponding XML comments whenever a signature or behavior changes; describe every parameter and any return value.
- Keep XML documentation in Russian, including source emitted by the generator.
- Do not ignore CS1591 or suppress it through NoWarn. Keep documentation generation and CS1591 enforcement scoped to the two library projects.
- Preserve the runtime XML files under lib/net9.0 and lib/net10.0 and the generator DLL and XML under analyzers/dotnet/cs in the NuGet package.

## Mandatory formatting before every commit

- Before every commit, Codex must run `dotnet format Osk.WolverineFx.RabbitMq.OAuth2.slnx` from the repository root. If the solution is renamed, use its actual filename.
- After formatting, run `dotnet format Osk.WolverineFx.RabbitMq.OAuth2.slnx --verify-no-changes` (using the same actual solution filename) and require it to succeed before committing.
- Include all changes made by formatting in the commit.
- If formatting or verification fails, fix the cause and rerun both commands successfully before committing.
- Never use `--no-verify` or any other means of bypassing established checks.
- Codex must follow this rule during all subsequent work. Keep it in AGENTS.md; do not add Git hooks or third-party formatting tools.

## Validation

Run from the repository root:

```sh
dotnet build Osk.WolverineFx.RabbitMq.OAuth2.slnx -c Release
dotnet run --project samples/StartupSmoke -c Release
dotnet test --project tests/Osk.WolverineFx.RabbitMq.OAuth2.Tests -c Release
dotnet pack src/Osk.WolverineFx.RabbitMq.OAuth2 -c Release -o artifacts
bash tools/verify-package-consumer.sh
dotnet test --project tests/Osk.WolverineFx.RabbitMq.OAuth2.IntegrationTests -c Release
```

Tests use TUnit and the Microsoft Testing Platform runner selected in global.json. Preserve the `--project` option for .NET 10 MTP commands; do not add xUnit or VSTest adapters. Process-wide Harmony registration tests must remain nonparallel.

The in-repository smoke example references the runtime project and the generator project (OutputItemType=Analyzer, ReferenceOutputAssembly=false). Keep solution builds independent of this unpublished package. The separate consumer verification script must retain its PackageReference to validate the actual NuGet artifact.

The verification script builds a consumer outside this checkout with an isolated NuGet cache stored outside its source directory. It must print `PASS: NuGet generator installed both Harmony patches before Main.` without calling Install explicitly or inheriting repository props files.

Compilation covers net9.0 and net10.0; unit tests, integration tests and the smoke consumer run on net10.0. Unit tests use mocked connections. Integration tests require a local Docker Engine and pull pinned RabbitMQ/Keycloak images via Testcontainers; do not skip them silently when Docker is unavailable. Share one dynamically mapped Keycloak/RabbitMQ pair and network through class hooks. Regular integration tests use independent hosts, queues, connection names and providers with a parallel limit of four. Keep any test making the shared Keycloak unavailable exclusive with an unkeyed NotInParallel attribute; the Slow category distinguishes this real endpoint outage from a per-provider simulated failure.

Use generated test credentials. Start Keycloak with start-dev over HTTP on container port 8080, fetch a real test JWT and JWKS over HTTP, select the RSA signing key by the token header's kid and export only its public parameters to PEM. Start RabbitMQ only after this key is ready and copy it through Testcontainers resource mapping. Configure auth_oauth2.signing_keys.<kid> instead of issuer/JWKS URLs or certificate infrastructure. Keep AMQP on 5672 and the HTTP Management API on 15672. This HTTP configuration is for integration tests only. Keycloak owns the generated private key; never commit a fixed private key. The public key is captured once per fixture; key rotation during a run is outside its scope. Preserve real JWT signature, audience, expiration and permissions checks, and keep RejectsInvalidToken corrupting only the signature. Do not add a Basic Authentication fallback for test AMQP connections.

Worker completion diagnostics depend on the pinned OAuth2 2.0.0 private task field and registration/registry fields; review that helper when upgrading. Own HTTP request timeouts must become retryable failures, while shutdown cancellation remains cancellation. Do not claim real RabbitMQ/OAuth2 integration is verified without a successful run. Do not publish to NuGet.org unless the user requests publication.
