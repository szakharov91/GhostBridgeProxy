using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Options;

namespace GhostBridgeProxy.Server;

public sealed class TunnelTcpListenerHostedService : BackgroundService
{
    private readonly TunnelRegistry _registry;
    private readonly IOptionsMonitor<TunnelOptions> _options;
    private readonly ILogger<TunnelTcpListenerHostedService> _logger;

    public TunnelTcpListenerHostedService(
        TunnelRegistry registry,
        IOptionsMonitor<TunnelOptions> options,
        ILogger<TunnelTcpListenerHostedService> logger)
    {
        _registry = registry;
        _options = options;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var port = _options.CurrentValue.PublicTcpPort;
        var listener = new TcpListener(IPAddress.Any, port);
        listener.Start();
        _logger.LogInformation("Tunnel public TCP listener on port {Port}", port);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                TcpClient tcp;
                try
                {
                    tcp = await listener.AcceptTcpClientAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Accept failed");
                    await Task.Delay(500, stoppingToken).ConfigureAwait(false);
                    continue;
                }

                _ = Task.Run(() => _registry.HandleInboundTcpAsync(tcp, stoppingToken), stoppingToken);
            }
        }
        finally
        {
            listener.Stop();
        }
    }
}
