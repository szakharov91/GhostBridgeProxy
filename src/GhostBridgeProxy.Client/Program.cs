using System.Buffers;
using System.Net.Sockets;
using System.Net.WebSockets;
using GhostBridgeProxy.Common;

namespace GhostBridgeProxy.Client;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        if (args.Length < 2)
        {
            Console.Error.WriteLine("Usage: GhostBridgeProxy.Client <ws-url> <local-host:port> [token]");
            Console.Error.WriteLine("Example: GhostBridgeProxy.Client ws://127.0.0.1:5080/tunnel 127.0.0.1:22");
            return 1;
        }

        var wsUrl = args[0];
        if (!TryParseHostPort(args[1], out var localHost, out var localPort))
        {
            Console.Error.WriteLine("Invalid local address. Use host:port (e.g. 127.0.0.1:3306).");
            return 1;
        }

        var token = args.Length >= 3 ? args[2] : null;

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        try
        {
            await RunTunnelAsync(wsUrl, localHost, localPort, token, cts.Token).ConfigureAwait(false);
            return 0;
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
            return 2;
        }
    }

    private static bool TryParseHostPort(string text, out string host, out int port)
    {
        host = "";
        port = 0;
        var colon = text.LastIndexOf(':');
        if (colon <= 0 || colon == text.Length - 1)
            return false;
        host = text[..colon];
        return int.TryParse(text[(colon + 1)..], out port) && port is > 0 and <= 65535;
    }

    private static async Task RunTunnelAsync(string wsUrl, string localHost, int localPort, string? token, CancellationToken ct)
    {
        using var ws = new ClientWebSocket();
        if (!string.IsNullOrEmpty(token))
            ws.Options.SetRequestHeader("X-Tunnel-Token", token);

        var uri = new Uri(wsUrl);
        await ws.ConnectAsync(uri, ct).ConfigureAwait(false);

        using var wsSendLock = new SemaphoreSlim(1, 1);

        var register = TunnelProtocol.BuildRegister(localHost, localPort);
        await SendTunnelFrameAsync(ws, wsSendLock, register, ct).ConfigureAwait(false);

        var recvBuffer = new TunnelReceiveBuffer();
        var streams = new Dictionary<uint, TcpStreamPump>();
        var socketScratch = new byte[64 * 1024];

        try
        {
            while (ws.State == WebSocketState.Open && !ct.IsCancellationRequested)
            {
                var received = await ws.ReceiveAsync(socketScratch.AsMemory(0, socketScratch.Length), ct)
                    .ConfigureAwait(false);

                if (received.MessageType == WebSocketMessageType.Close)
                    break;

                if (received.MessageType != WebSocketMessageType.Binary)
                    continue;

                recvBuffer.Append(socketScratch.AsSpan(0, received.Count));

                while (recvBuffer.TryTakeFrame(out var frame))
                {
                    switch (frame.Type)
                    {
                        case TunnelMessageType.OpenStream:
                            await HandleOpenStreamAsync(ws, wsSendLock, streams, frame.StreamId, localHost, localPort, ct)
                                .ConfigureAwait(false);
                            break;

                        case TunnelMessageType.Data:
                            if (streams.TryGetValue(frame.StreamId, out var pump))
                                await pump.EnqueueFromTunnelAsync(frame.Payload, ct).ConfigureAwait(false);
                            break;

                        case TunnelMessageType.CloseStream:
                            await RemoveStreamAsync(streams, frame.StreamId).ConfigureAwait(false);
                            break;

                        case TunnelMessageType.Register:
                            break;
                    }
                }
            }
        }
        finally
        {
            foreach (var id in streams.Keys.ToArray())
                await RemoveStreamAsync(streams, id).ConfigureAwait(false);
        }
    }

    private static async Task SendTunnelFrameAsync(ClientWebSocket ws, SemaphoreSlim sendLock, byte[] frame, CancellationToken ct)
    {
        await sendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await ws.SendAsync(frame, WebSocketMessageType.Binary, endOfMessage: true, ct).ConfigureAwait(false);
        }
        finally
        {
            sendLock.Release();
        }
    }

    private static async Task HandleOpenStreamAsync(
        ClientWebSocket ws,
        SemaphoreSlim wsSendLock,
        Dictionary<uint, TcpStreamPump> streams,
        uint streamId,
        string localHost,
        int localPort,
        CancellationToken ct)
    {
        if (streams.ContainsKey(streamId))
            return;

        var tcp = new TcpClient();
        try
        {
            await tcp.ConnectAsync(localHost, localPort, ct).ConfigureAwait(false);
        }
        catch (Exception)
        {
            tcp.Dispose();
            var closeFrame = TunnelProtocol.BuildFrame(TunnelMessageType.CloseStream, streamId, ReadOnlySpan<byte>.Empty);
            await SendTunnelFrameAsync(ws, wsSendLock, closeFrame, ct).ConfigureAwait(false);
            return;
        }

        var pump = new TcpStreamPump(ws, wsSendLock, streamId, tcp);
        streams[streamId] = pump;
        _ = pump.RunLocalToTunnelAsync(ct);
    }

    private static async Task RemoveStreamAsync(Dictionary<uint, TcpStreamPump> streams, uint streamId)
    {
        if (!streams.Remove(streamId, out var pump))
            return;
        await pump.DisposeAsync().ConfigureAwait(false);
    }
}

internal sealed class TcpStreamPump : IAsyncDisposable
{
    private readonly ClientWebSocket _webSocket;
    private readonly SemaphoreSlim _wsSendLock;
    private readonly uint _streamId;
    private readonly TcpClient _tcp;
    private readonly NetworkStream _networkStream;

    public TcpStreamPump(ClientWebSocket webSocket, SemaphoreSlim wsSendLock, uint streamId, TcpClient tcp)
    {
        _webSocket = webSocket;
        _wsSendLock = wsSendLock;
        _streamId = streamId;
        _tcp = tcp;
        _networkStream = tcp.GetStream();
    }

    public async Task EnqueueFromTunnelAsync(ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        await _networkStream.WriteAsync(payload, ct).ConfigureAwait(false);
    }

    public async Task RunLocalToTunnelAsync(CancellationToken ct)
    {
        var pool = ArrayPool<byte>.Shared;
        var buf = pool.Rent(65536);
        try
        {
            while (!ct.IsCancellationRequested && _webSocket.State == WebSocketState.Open)
            {
                var n = await _networkStream.ReadAsync(buf.AsMemory(0, buf.Length), ct).ConfigureAwait(false);
                if (n == 0)
                    break;

                var frame = TunnelProtocol.BuildFrame(TunnelMessageType.Data, _streamId, buf.AsSpan(0, n));
                await _wsSendLock.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    await _webSocket.SendAsync(frame, WebSocketMessageType.Binary, endOfMessage: true, ct)
                        .ConfigureAwait(false);
                }
                finally
                {
                    _wsSendLock.Release();
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (WebSocketException) { }
        catch (IOException) { }
        finally
        {
            try
            {
                var close = TunnelProtocol.BuildFrame(TunnelMessageType.CloseStream, _streamId, ReadOnlySpan<byte>.Empty);
                await _wsSendLock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
                try
                {
                    if (_webSocket.State == WebSocketState.Open)
                        await _webSocket.SendAsync(close, WebSocketMessageType.Binary, endOfMessage: true, CancellationToken.None)
                            .ConfigureAwait(false);
                }
                finally
                {
                    _wsSendLock.Release();
                }
            }
            catch
            {
                // ignore
            }

            pool.Return(buf);
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await _networkStream.DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
            // ignore
        }

        _tcp.Dispose();
    }
}
