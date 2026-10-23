using System.Net;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Patch.IntegrationTests;

/// <summary>
/// Создаёт временный CA и сертификат Keycloak для HTTPS интеграционного окружения.
/// </summary>
/// <remarks>
/// Системное хранилище доверия не изменяется. Клиент проверяет имя сервера, цепочку до собственного CA и назначение server authentication.
/// </remarks>
internal sealed class TestTls : IDisposable
{
    private readonly X509Certificate2 _root;
    /// <summary>
    /// PEM-сертификат CA, которому доверяют RabbitMQ и тестовые HTTP-клиенты.
    /// </summary>
    public byte[] CaPem { get; }
    /// <summary>
    /// PEM-сертификат сервера с именами Keycloak, localhost и loopback-адресами.
    /// </summary>
    public byte[] ServerCertificatePem { get; }
    /// <summary>
    /// Приватный ключ сервера в PEM; используется только для настройки тестового Keycloak.
    /// </summary>
    public byte[] ServerKeyPem { get; }

    /// <summary>
    /// Генерирует RSA-ключи, краткоживущий CA и подписанный им сертификат Keycloak.
    /// </summary>
    public TestTls()
    {
        using var rootKey = RSA.Create(2048);
        var rootRequest = new CertificateRequest("CN=Wolverine Integration CA", rootKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        rootRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        rootRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign, true));
        using var issuer = rootRequest.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(1));
        _root = X509CertificateLoader.LoadCertificate(issuer.Export(X509ContentType.Cert));
        CaPem = System.Text.Encoding.UTF8.GetBytes(_root.ExportCertificatePem());

        using var serverKey = RSA.Create(2048);
        var request = new CertificateRequest("CN=keycloak", serverKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { new("1.3.6.1.5.5.7.3.1") }, false));
        var names = new SubjectAlternativeNameBuilder();
        names.AddDnsName("keycloak");
        names.AddDnsName("localhost");
        names.AddIpAddress(IPAddress.Loopback);
        names.AddIpAddress(IPAddress.IPv6Loopback);
        request.CertificateExtensions.Add(names.Build());
        using var certificate = request.Create(issuer, DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddHours(12), RandomNumberGenerator.GetBytes(16));
        ServerCertificatePem = System.Text.Encoding.UTF8.GetBytes(certificate.ExportCertificatePem());
        ServerKeyPem = System.Text.Encoding.UTF8.GetBytes(serverKey.ExportPkcs8PrivateKeyPem());
    }

    /// <summary>
    /// Создаёт HTTPS handler с проверкой имени сервера, собственного CA и server-auth EKU.
    /// </summary>
    /// <remarks>
    /// У временного CA нет сервиса отзыва, поэтому revocation-проверка отключена только для этого handler; проверка цепочки и имени сервера сохраняется.
    /// </remarks>
    /// <returns>Новый handler, за освобождение которого отвечает вызывающий код.</returns>
    public HttpClientHandler CreateHandler() => new()
    {
        UseProxy = false,
        ServerCertificateCustomValidationCallback = (_, certificate, _, errors) =>
        {
            if (certificate is null || (errors & (SslPolicyErrors.RemoteCertificateNotAvailable | SslPolicyErrors.RemoteCertificateNameMismatch)) != 0)
                return false;
            using var chain = new X509Chain();
            chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            chain.ChainPolicy.CustomTrustStore.Add(_root);
            // This ephemeral local CA has no revocation service.
            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            chain.ChainPolicy.ApplicationPolicy.Add(new Oid("1.3.6.1.5.5.7.3.1"));
            return chain.Build(certificate);
        }
    };

    /// <summary>
    /// Освобождает объект сертификата временного CA.
    /// </summary>
    public void Dispose() => _root.Dispose();
}
