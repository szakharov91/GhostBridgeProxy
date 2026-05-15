namespace GhostBridgeProxy.Common;

public enum TunnelMessageType : byte
{
    Register = 0,
    OpenStream = 1,
    Data = 2,
    CloseStream = 3,
}
