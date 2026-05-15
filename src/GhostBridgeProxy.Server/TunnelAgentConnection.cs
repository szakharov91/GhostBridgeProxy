using System.Net.WebSockets;
using GhostBridgeProxy.Common;
using Microsoft.Extensions.Logging;

namespace GhostBridgeProxy.Server;

public static class TunnelAgentConnection
{
    public static async Task RunAsync(WebSocket webSocket, TunnelRegistry registry, ILogger logger, CancellationToken cancellationToken)
    {
        var recvBuffer = new TunnelReceiveBuffer();
        var socketScratch = new byte[64 * 1024];
        TunnelAgentSession? session = null;
        var registered = false;

        try
        {
            while (webSocket.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
            {
                var received = await webSocket.ReceiveAsync(socketScratch.AsMemory(0, socketScratch.Length), cancellationToken)
                    .ConfigureAwait(false);

                if (received.MessageType == WebSocketMessageType.Close)
                    break;

                if (received.MessageType != WebSocketMessageType.Binary)
                    continue;

                recvBuffer.Append(socketScratch.AsSpan(0, received.Count));

                while (recvBuffer.TryTakeFrame(out var frame))
                {
                    if (!registered)
                    {
                        if (frame.Type != TunnelMessageType.Register)
                        {
                            await webSocket.CloseAsync(WebSocketCloseStatus.PolicyViolation, "First frame must be Register.",
                                CancellationToken.None).ConfigureAwait(false);
                            return;
                        }

                        if (!TunnelProtocol.TryParseRegisterPayload(frame.Payload.Span, out var host, out var port))
                        {
                            await webSocket.CloseAsync(WebSocketCloseStatus.InvalidPayloadData, "Invalid Register payload.",
                                CancellationToken.None).ConfigureAwait(false);
                            return;
                        }

                        logger.LogInformation("Tunnel agent registered for target {Host}:{Port}", host, port);
                        session = new TunnelAgentSession(webSocket);
                        registry.AttachAgent(session);
                        registered = true;
                        continue;
                    }

                    await HandleAgentFrameAsync(session!, frame, logger, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        catch (WebSocketException ex)
        {
            logger.LogDebug(ex, "WebSocket ended");
        }
        finally
        {
            if (session is not null)
            {
                registry.DetachAgentIfCurrent(session);
                foreach (var kv in session.Streams.ToArray())
                {
                    if (session.Streams.TryRemove(kv.Key, out var tcp))
                        tcp.Dispose();
                }
            }
        }
    }

    private static async Task HandleAgentFrameAsync(TunnelAgentSession session, TunnelFrame frame, ILogger logger, CancellationToken ct)
    {
        switch (frame.Type)
        {
            case TunnelMessageType.Data:
                if (!session.Streams.TryGetValue(frame.StreamId, out var tcp))
                    return;
                try
                {
                    var stream = tcp.GetStream();
                    await stream.WriteAsync(frame.Payload, ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger.LogDebug(ex, "Write to public TCP failed for stream {StreamId}", frame.StreamId);
                    if (session.Streams.TryRemove(frame.StreamId, out var removed))
                        removed.Dispose();
                }
                break;

            case TunnelMessageType.CloseStream:
                if (session.Streams.TryRemove(frame.StreamId, out var toClose))
                    toClose.Dispose();
                break;

            case TunnelMessageType.OpenStream:
            case TunnelMessageType.Register:
                break;
        }
    }
}
