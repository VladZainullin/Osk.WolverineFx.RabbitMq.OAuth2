using System.Net;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Osk.WolverineFx.RabbitMq.OAuth2.Patch.IntegrationTests;

// An isolated ephemeral CA; neither the OS trust store nor TLS checks are changed.
internal sealed class TestTls : IDisposable
{
    private readonly X509Certificate2 _root;
    public byte[] CaPem { get; }
    public byte[] ServerCertificatePem { get; }
    public byte[] ServerKeyPem { get; }

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

    public void Dispose() => _root.Dispose();
}
