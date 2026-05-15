namespace GhostBridgeProxy.Server;

public sealed class TunnelOptions
{
    public const string SectionName = "Tunnel";

    public int ControlListenPort { get; set; } = 5080;
    public int PublicTcpPort { get; set; } = 23000;
    public string? SharedSecret { get; set; }
}
