using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Patch.IntegrationTests;

internal sealed class OAuth2Environment : IAsyncDisposable
{
    public const int TokenLifetimeSeconds = 8;
    private readonly INetwork _network = new NetworkBuilder().Build();
    public TestTls Tls { get; } = new();
    private readonly string _adminPassword = Guid.NewGuid().ToString("N");
    private Task? _disposeTask;
    public string ClientSecret { get; } = Guid.NewGuid().ToString("N");
    public IContainer Keycloak { get; }
    public IContainer RabbitMq { get; }
    public HttpClient Management { get; } = new() { Timeout = TimeSpan.FromSeconds(10) };
    public Uri TokenEndpoint => new($"https://{Keycloak.Hostname}:{Keycloak.GetMappedPublicPort(8443)}/realms/wolverine/protocol/openid-connect/token");

    public OAuth2Environment()
    {
        // Reserve a random fixed binding so Stop/Start keeps the OAuth2 client's endpoint stable.
        using var portReservation = new TcpListener(IPAddress.Loopback, 0);
        portReservation.Start();
        var keycloakPort = ((IPEndPoint)portReservation.LocalEndpoint).Port;
        portReservation.Stop();

        var realm = JsonSerializer.SerializeToUtf8Bytes(new
        {
            realm = "wolverine", enabled = true, accessTokenLifespan = TokenLifetimeSeconds,
            clients = new[]
            {
                new
                {
                    clientId = "wolverine", secret = ClientSecret, enabled = true,
                    publicClient = false, serviceAccountsEnabled = true, standardFlowEnabled = false,
                    protocol = "openid-connect",
                    protocolMappers = new[]
                    {
                        new
                        {
                            name = "rabbitmq-permissions", protocol = "openid-connect", protocolMapper = "oidc-hardcoded-claim-mapper",
                            config = new Dictionary<string, string>
                            {
                                ["claim.name"] = "permissions", ["claim.value"] = "wolverine.configure:*/* wolverine.write:*/* wolverine.read:*/*",
                                ["jsonType.label"] = "String", ["access.token.claim"] = "true"
                            }
                        },
                        new
                        {
                            name = "rabbitmq-audience", protocol = "openid-connect", protocolMapper = "oidc-audience-mapper",
                            config = new Dictionary<string, string>
                            {
                                ["included.custom.audience"] = "wolverine", ["access.token.claim"] = "true"
                            }
                        }
                    }
                }
            }
        });

        Keycloak = new ContainerBuilder("quay.io/keycloak/keycloak:26.1.4")
            .WithNetwork(_network).WithNetworkAliases("keycloak")
            .WithPortBinding(keycloakPort, 8443)
            .WithEnvironment("KC_HOSTNAME", "https://keycloak:8443")
            .WithEnvironment("JAVA_OPTS_KC_HEAP", "-Xms128m -Xmx512m")
            .WithEnvironment("JAVA_OPTS_APPEND", "-XX:ActiveProcessorCount=2")
            .WithResourceMapping(realm, "/opt/keycloak/data/import/wolverine.json")
            .WithResourceMapping(Tls.ServerCertificatePem, "/opt/keycloak/conf/server.crt")
            .WithResourceMapping(Tls.ServerKeyPem, "/opt/keycloak/conf/server.key")
            .WithCommand("start-dev", "--import-realm", "--http-enabled=false",
                "--https-certificate-file=/opt/keycloak/conf/server.crt", "--https-certificate-key-file=/opt/keycloak/conf/server.key")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Listening on: .*https://"))
            .Build();

        RabbitMq = new ContainerBuilder("rabbitmq:4.1.4-management")
            .WithNetwork(_network).WithNetworkAliases("rabbitmq")
            .WithPortBinding(5672, true).WithPortBinding(15672, true)
            .WithEnvironment("RABBITMQ_DEFAULT_USER", "integration-admin")
            .WithEnvironment("RABBITMQ_DEFAULT_PASS", _adminPassword)
            .WithEnvironment("RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS", "+S 2:2 +A 4")
            .WithResourceMapping(Encoding.UTF8.GetBytes("[rabbitmq_management,rabbitmq_auth_backend_oauth2].\n"), "/etc/rabbitmq/enabled_plugins")
            .WithResourceMapping(Encoding.UTF8.GetBytes("""
                auth_backends.1 = oauth2
                auth_backends.2 = internal
                auth_oauth2.resource_server_id = wolverine
                auth_oauth2.additional_scopes_key = permissions
                auth_oauth2.issuer = https://keycloak:8443/realms/wolverine
                auth_oauth2.jwks_url = https://keycloak:8443/realms/wolverine/protocol/openid-connect/certs
                auth_oauth2.https.cacertfile = /etc/rabbitmq/ca.crt
                auth_oauth2.https.verify = verify_peer
                loopback_users.guest = false
                collect_statistics_interval = 1000
                """), "/etc/rabbitmq/rabbitmq.conf")
            .WithResourceMapping(Tls.CaPem, "/etc/rabbitmq/ca.crt")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server startup complete"))
            .Build();
    }

    public async Task StartAsync(CancellationToken ct)
    {
        try
        {
            await _network.CreateAsync(ct);
            await Keycloak.StartAsync(ct);
            await WaitForKeycloakAsync(ct);
            await RabbitMq.StartAsync(ct);
            Management.BaseAddress = new Uri($"http://{RabbitMq.Hostname}:{RabbitMq.GetMappedPublicPort(15672)}/api/");
            Management.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
                Convert.ToBase64String(Encoding.UTF8.GetBytes($"integration-admin:{_adminPassword}")));
            await WaitUntilAsync(async () =>
                {
                    using var response = await Management.GetAsync("overview", ct);
                    return response.IsSuccessStatusCode;
                },
                TimeSpan.FromSeconds(30), ct);
        }
        catch
        {
            // Startup diagnostics are collected before application token requests begin.
            foreach (var container in new[] { Keycloak, RabbitMq })
            {
                try
                {
                    var logs = await container.GetLogsAsync();
                    Console.Error.WriteLine(logs.Stdout + logs.Stderr);
                }
                catch (InvalidOperationException) { /* The container has not been created yet. */ }
            }
            await DisposeAsync();
            throw;
        }
    }

    public async Task<string[]> ConnectionIdsAsync(string connectionName)
    {
        using var response = await Management.GetAsync("connections");
        response.EnsureSuccessStatusCode();
        using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return json.RootElement.EnumerateArray()
            .Where(c => c.GetProperty("client_properties").TryGetProperty("connection_name", out var name) && name.GetString() == connectionName)
            .Select(c => $"{c.GetProperty("name").GetString()}:{c.GetProperty("connected_at").GetInt64()}")
            .Order().ToArray();
    }

    public async Task<long> DeliveredMessagesAsync(string queue, CancellationToken ct)
    {
        using var response = await Management.GetAsync($"queues/%2F/{Uri.EscapeDataString(queue)}", ct);
        response.EnsureSuccessStatusCode();
        using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        return json.RootElement.TryGetProperty("message_stats", out var stats) && stats.TryGetProperty("deliver_get", out var deliveries)
            ? deliveries.GetInt64() : 0;
    }

    public async Task WaitForKeycloakAsync(CancellationToken ct)
    {
        using var http = new HttpClient(Tls.CreateHandler()) { Timeout = TimeSpan.FromSeconds(2) };
        var realm = new Uri($"https://{Keycloak.Hostname}:{Keycloak.GetMappedPublicPort(8443)}/realms/wolverine");
        await WaitUntilAsync(async () =>
        {
            try
            {
                using var response = await http.GetAsync(realm, ct);
                return response.IsSuccessStatusCode;
            }
            catch (HttpRequestException) { return false; }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested) { return false; }
        }, TimeSpan.FromSeconds(90), ct);
    }

    public static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout, CancellationToken ct = default)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
        deadline.CancelAfter(timeout);
        try
        {
            while (!await condition()) await Task.Delay(100, deadline.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new TimeoutException($"Expected condition was not met within {timeout}.");
        }
    }

    public ValueTask DisposeAsync() => new(_disposeTask ??= DisposeCoreAsync());

    private async Task DisposeCoreAsync()
    {
        Management.Dispose();
        try { await RabbitMq.DisposeAsync(); }
        finally
        {
            try { await Keycloak.DisposeAsync(); }
            finally
            {
                try { await _network.DisposeAsync(); }
                finally { Tls.Dispose(); }
            }
        }
    }
}
