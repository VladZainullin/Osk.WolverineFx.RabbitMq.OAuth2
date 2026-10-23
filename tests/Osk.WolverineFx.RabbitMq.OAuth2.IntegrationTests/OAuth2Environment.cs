using System.Buffers.Text;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace Osk.WolverineFx.RabbitMq.OAuth2.IntegrationTests;

/// <summary>
/// Владеет общей Docker-сетью, Keycloak и RabbitMQ с локальным публичным ключом для интеграционного класса.
/// </summary>
/// <remarks>
/// Контейнеры используют динамические публичные порты. Management API настраивается до старта тестов и допускает параллельные запросы без изменения заголовков.
/// </remarks>
internal sealed class OAuth2Environment : IAsyncDisposable
{
    /// <summary>
    /// Срок действия JWT, выдаваемых тестовым realm Keycloak, в секундах.
    /// </summary>
    public const int TokenLifetimeSeconds = 8;
    private readonly long _constructionStarted = Stopwatch.GetTimestamp();
    private readonly INetwork _network = new NetworkBuilder().Build();
    private readonly string _adminPassword = Guid.NewGuid().ToString("N");
    private IContainer? _rabbitMq;
    private Task? _disposeTask;
    /// <summary>
    /// Сгенерированный secret общего service-account client; значение не должно попадать в журналы.
    /// </summary>
    public string ClientSecret { get; } = Guid.NewGuid().ToString("N");
    /// <summary>
    /// Общий контейнер Keycloak; обычные тесты не изменяют его состояние.
    /// </summary>
    public IContainer Keycloak { get; }
    /// <summary>
    /// Общий контейнер RabbitMQ с OAuth2 backend и Management API.
    /// </summary>
    public IContainer RabbitMq => _rabbitMq ?? throw new InvalidOperationException("RabbitMQ has not been prepared");
    /// <summary>
    /// HTTP-клиент Management API с административными credentials для наблюдения состояния брокера.
    /// </summary>
    public HttpClient Management { get; } = new() { Timeout = TimeSpan.FromSeconds(10) };
    /// <summary>
    /// HTTP-адрес выдачи токенов через динамический публичный порт запущенного Keycloak.
    /// </summary>
    public Uri TokenEndpoint => new($"http://{Keycloak.Hostname}:{Keycloak.GetMappedPublicPort(8080)}/realms/wolverine/protocol/openid-connect/token");

    /// <summary>
    /// Генерирует тестовые credentials и описывает контейнер Keycloak без его запуска.
    /// </summary>
    public OAuth2Environment()
    {
        Management.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
            Convert.ToBase64String(Encoding.UTF8.GetBytes($"integration-admin:{_adminPassword}")));

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
            .WithPortBinding(8080, true)
            .WithEnvironment("KC_HOSTNAME", "http://keycloak:8080")
            .WithEnvironment("JAVA_OPTS_KC_HEAP", "-Xms128m -Xmx512m")
            .WithEnvironment("JAVA_OPTS_APPEND", "-XX:ActiveProcessorCount=2")
            .WithResourceMapping(realm, "/opt/keycloak/data/import/wolverine.json")
            .WithCommand("start-dev", "--import-realm")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Listening on: .*http://"))
            .Build();

        WriteTiming("Fixture construction", _constructionStarted);
    }

    /// <summary>
    /// Создаёт сеть, запускает Keycloak, получает публичный ключ и затем запускает RabbitMQ с проверками готовности сервисов.
    /// </summary>
    /// <remarks>
    /// При неудачном запуске собирает журналы и освобождает созданные ресурсы.
    /// </remarks>
    /// <param name="ct">Токен отмены запуска и readiness-проверок.</param>
    /// <returns>Задача подготовки окружения к выполнению тестов.</returns>
    public async Task StartAsync(CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            var networkStarted = Stopwatch.GetTimestamp();
            await _network.CreateAsync(ct);
            WriteTiming("Network creation", networkStarted);
            await StartKeycloakAsync(ct);
            await StartRabbitMqAsync(ct);
        }
        catch
        {
            // Startup diagnostics are collected before application token requests begin.
            foreach (var container in new[] { Keycloak, _rabbitMq }.OfType<IContainer>())
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
        finally { WriteTiming("Fixture startup", started); }
    }

    /// <summary>
    /// Запускает Keycloak и ожидает доступности импортированного realm по HTTP.
    /// </summary>
    /// <param name="ct">Токен отмены запуска и HTTP-проверок.</param>
    /// <returns>Задача готовности Keycloak.</returns>
    private async Task StartKeycloakAsync(CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            await Keycloak.StartAsync(ct);
            await WaitForKeycloakAsync(ct);
        }
        finally { WriteTiming("Keycloak start and readiness", started); }
    }

    /// <summary>
    /// Получает публичный ключ Keycloak, запускает RabbitMQ, настраивает адрес Management API и ожидает ответа overview.
    /// </summary>
    /// <param name="ct">Токен отмены запуска и HTTP-проверок.</param>
    /// <returns>Задача готовности RabbitMQ.</returns>
    private async Task StartRabbitMqAsync(CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            var (kid, publicKey) = await GetSigningKeyAsync(ct);
            _rabbitMq = new ContainerBuilder("rabbitmq:4.1.4-management")
                .WithNetwork(_network).WithNetworkAliases("rabbitmq")
                .WithPortBinding(5672, true).WithPortBinding(15672, true)
                .WithEnvironment("RABBITMQ_DEFAULT_USER", "integration-admin")
                .WithEnvironment("RABBITMQ_DEFAULT_PASS", _adminPassword)
                .WithEnvironment("RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS", "+S 2:2 +A 4")
                .WithResourceMapping(Encoding.UTF8.GetBytes("[rabbitmq_management,rabbitmq_auth_backend_oauth2].\n"), "/etc/rabbitmq/enabled_plugins")
                .WithResourceMapping(Encoding.UTF8.GetBytes($"""
                    auth_backends.1 = oauth2
                    auth_backends.2 = internal
                    auth_oauth2.resource_server_id = wolverine
                    auth_oauth2.additional_scopes_key = permissions
                    auth_oauth2.signing_keys.{kid} = /etc/rabbitmq/keycloak-public.pem
                    loopback_users.guest = false
                    collect_statistics_interval = 1000
                    """), "/etc/rabbitmq/rabbitmq.conf")
                .WithResourceMapping(publicKey, "/etc/rabbitmq/keycloak-public.pem")
                .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server startup complete"))
                .Build();
            await RabbitMq.StartAsync(ct);
            Management.BaseAddress = new Uri($"http://{RabbitMq.Hostname}:{RabbitMq.GetMappedPublicPort(15672)}/api/");
            await WaitUntilAsync(async () =>
            {
                try
                {
                    using var response = await Management.GetAsync("overview", ct);
                    return response.IsSuccessStatusCode;
                }
                catch (HttpRequestException) { return false; }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested) { return false; }
            }, TimeSpan.FromSeconds(30), ct);
        }
        finally { WriteTiming("RabbitMQ start and readiness", started); }
    }

    /// <summary>
    /// Получает тестовый JWT и JWKS по HTTP и экспортирует публичный RSA-ключ подписи, выбранный по kid токена.
    /// </summary>
    /// <param name="ct">Токен отмены HTTP-запросов.</param>
    /// <returns>Идентификатор ключа и публичный ключ в PEM для настройки RabbitMQ.</returns>
    private async Task<(string Kid, byte[] PublicKey)> GetSigningKeyAsync(CancellationToken ct)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        using var form = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials", ["client_id"] = "wolverine", ["client_secret"] = ClientSecret
        });
        using var response = await http.PostAsync(TokenEndpoint, form, ct);
        response.EnsureSuccessStatusCode();
        using var token = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        var jwt = token.RootElement.GetProperty("access_token").GetString()!;
        using var header = JsonDocument.Parse(Base64Url.DecodeFromChars(jwt.Split('.')[0]));
        var kid = header.RootElement.GetProperty("kid").GetString()!;
        if (header.RootElement.GetProperty("alg").GetString() != "RS256")
            throw new InvalidOperationException("Expected an RS256 test token from Keycloak");

        using var stream = await http.GetStreamAsync(new Uri(TokenEndpoint, "certs"), ct);
        using var jwks = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
        // Match the signing token's kid: the realm also exposes keys used for encryption.
        var key = jwks.RootElement.GetProperty("keys").EnumerateArray().Single(key =>
            key.GetProperty("kid").GetString() == kid && key.GetProperty("kty").GetString() == "RSA"
            && key.GetProperty("use").GetString() == "sig");
        using var rsa = RSA.Create();
        rsa.ImportParameters(new RSAParameters
        {
            Modulus = Base64Url.DecodeFromChars(key.GetProperty("n").GetString()!),
            Exponent = Base64Url.DecodeFromChars(key.GetProperty("e").GetString()!)
        });
        return (kid, Encoding.UTF8.GetBytes(rsa.ExportSubjectPublicKeyInfoPem()));
    }

    /// <summary>
    /// Возвращает идентификаторы брокерных соединений с указанным ClientProvidedName.
    /// </summary>
    /// <param name="connectionName">Общее клиентское имя соединений одной тестовой сессии.</param>
    /// <returns>Отсортированные комбинации имени соединения и connected_at, позволяющие обнаружить повторное подключение.</returns>
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

    /// <summary>
    /// Читает накопительный счётчик deliver_get указанной очереди через Management API.
    /// </summary>
    /// <param name="queue">Имя очереди в виртуальном хосте по умолчанию.</param>
    /// <param name="ct">Токен отмены HTTP-запроса.</param>
    /// <returns>Число доставок или ноль, если брокер ещё не опубликовал message_stats.</returns>
    public async Task<long> DeliveredMessagesAsync(string queue, CancellationToken ct)
    {
        using var response = await Management.GetAsync($"queues/%2F/{Uri.EscapeDataString(queue)}", ct);
        response.EnsureSuccessStatusCode();
        using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        return json.RootElement.TryGetProperty("message_stats", out var stats) && stats.TryGetProperty("deliver_get", out var deliveries)
            ? deliveries.GetInt64() : 0;
    }

    /// <summary>
    /// Читает число consumers указанной очереди через Management API.
    /// </summary>
    /// <param name="queue">Имя очереди в виртуальном хосте по умолчанию.</param>
    /// <param name="ct">Токен отмены HTTP-запроса.</param>
    /// <returns>Число consumers по последней доступной статистике брокера.</returns>
    public async Task<int> ConsumerCountAsync(string queue, CancellationToken ct)
    {
        using var response = await Management.GetAsync($"queues/%2F/{Uri.EscapeDataString(queue)}", ct);
        response.EnsureSuccessStatusCode();
        using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        return json.RootElement.GetProperty("consumers").GetInt32();
    }

    /// <summary>
    /// Ожидает успешного HTTP-ответа импортированного realm после запуска или возобновления Keycloak.
    /// </summary>
    /// <param name="ct">Токен отмены ожидания.</param>
    /// <returns>Задача ожидания доступности realm.</returns>
    public async Task WaitForKeycloakAsync(CancellationToken ct)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
        var realm = new Uri($"http://{Keycloak.Hostname}:{Keycloak.GetMappedPublicPort(8080)}/realms/wolverine");
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

    /// <summary>
    /// Повторяет асинхронную проверку наблюдаемого состояния до успеха, отмены или истечения бюджета.
    /// </summary>
    /// <remarks>
    /// Проверки разделены интервалом 100 мс. Делегат должен самостоятельно ограничивать длительность своих асинхронных операций.
    /// </remarks>
    /// <param name="condition">Проверка, возвращающая true при достижении нужного состояния.</param>
    /// <param name="timeout">Бюджет ожидания между проверками.</param>
    /// <param name="ct">Внешний токен отмены ожидания.</param>
    /// <returns>Задача, завершающаяся после успешной проверки.</returns>
    /// <exception cref="TimeoutException">Условие не достигнуто до истечения бюджета ожидания.</exception>
    /// <exception cref="OperationCanceledException">Ожидание прервано внешним токеном отмены.</exception>
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

    /// <summary>
    /// Освобождает Management API клиент, оба контейнера и Docker-сеть.
    /// </summary>
    /// <returns>Операция освобождения ресурсов окружения.</returns>
    public ValueTask DisposeAsync() => new(_disposeTask ??= DisposeCoreAsync());

    /// <summary>
    /// Освобождает все принадлежащие окружению ресурсы, сохраняя очистку остальных при ошибке одного из этапов.
    /// </summary>
    /// <returns>Задача последовательного освобождения ресурсов.</returns>
    private async Task DisposeCoreAsync()
    {
        var started = Stopwatch.GetTimestamp();
        Management.Dispose();
        try { if (_rabbitMq is not null) await _rabbitMq.DisposeAsync(); }
        finally
        {
            try { await Keycloak.DisposeAsync(); }
            finally
            {
                try { await _network.DisposeAsync(); }
                finally { WriteTiming("Fixture disposal", started); }
            }
        }
    }

    /// <summary>
    /// Выводит длительность этапа инфраструктуры без значений credentials.
    /// </summary>
    /// <param name="phase">Название измеряемого этапа.</param>
    /// <param name="started">Начальная метка Stopwatch.</param>
    private static void WriteTiming(string phase, long started) =>
        Console.WriteLine($"[integration timing] {phase}: {Stopwatch.GetElapsedTime(started).TotalMilliseconds:F0} ms");
}
