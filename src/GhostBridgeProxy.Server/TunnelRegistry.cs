using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Net.WebSockets;
using GhostBridgeProxy.Common;

namespace GhostBridgeProxy.Server;

/// <summary>
/// MVP: один активный агент (WebSocket). Новое подключение заменяет предыдущее.
/// </summary>
public sealed class TunnelRegistry
{
    private readonly object _gate = new();
    private TunnelAgentSession? _session;
    private readonly ILogger<TunnelRegistry> _logger;

    public TunnelRegistry(ILogger<TunnelRegistry> logger) => _logger = logger;

    public TunnelAgentSession? GetSession()
    {
        lock (_gate)
            return _session;
    }

    /// <summary>Заменяет текущего агента и закрывает старый WebSocket.</summary>
    public void AttachAgent(TunnelAgentSession newSession)
    {
        TunnelAgentSession? old;
        lock (_gate)
        {
            old = _session;
            _session = newSession;
        }

        if (old is not null)
        {
            _logger.LogInformation("Replacing tunnel agent with a new connection.");
            _ = old.ShutdownAsync();
        }
    }

    public void DetachAgentIfCurrent(TunnelAgentSession session)
    {
        lock (_gate)
        {
            if (_session == session)
                _session = null;
        }
    }

    public async Task HandleInboundTcpAsync(TcpClient tcp, CancellationToken ct)
    {
        var session = GetSession();
        if (session is null)
        {
            _logger.LogWarning("Inbound TCP connection rejected: no tunnel agent connected.");
            tcp.Dispose();
            return;
        }

        var streamId = session.AllocateStreamId();
        try
        {
            var open = TunnelProtocol.BuildFrame(TunnelMessageType.OpenStream, streamId, ReadOnlySpan<byte>.Empty);
            await session.SendAsync(open, ct).ConfigureAwait(false);

            if (!session.Streams.TryAdd(streamId, tcp))
            {
                tcp.Dispose();
                return;
            }

            await PumpTcpToAgentAsync(session, tcp, streamId, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Public TCP pump ended for stream {StreamId}", streamId);
        }
        finally
        {
            if (session.Streams.TryRemove(streamId, out var removed))
                removed.Dispose();

            try
            {
                var close = TunnelProtocol.BuildFrame(TunnelMessageType.CloseStream, streamId, ReadOnlySpan<byte>.Empty);
                await session.SendAsync(close, CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                // ignore
            }
        }
    }

    private static async Task PumpTcpToAgentAsync(TunnelAgentSession session, TcpClient tcp, uint streamId, CancellationToken ct)
    {
        await using var net = tcp.GetStream();
        var buf = new byte[64 * 1024];
        while (true)
        {
            var n = await net.ReadAsync(buf.AsMemory(0, buf.Length), ct).ConfigureAwait(false);
            if (n == 0)
                break;

            var frame = TunnelProtocol.BuildFrame(TunnelMessageType.Data, streamId, buf.AsSpan(0, n));
            await session.SendAsync(frame, ct).ConfigureAwait(false);
        }
    }
}

public sealed class TunnelAgentSession
{
    private readonly WebSocket _webSocket;
    private int _nextStreamId;
    public ConcurrentDictionary<uint, TcpClient> Streams { get; } = new();
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    public TunnelAgentSession(WebSocket webSocket) => _webSocket = webSocket;

    public uint AllocateStreamId() => (uint)Interlocked.Increment(ref _nextStreamId);

    public async Task SendAsync(byte[] frame, CancellationToken cancellationToken)
    {
        await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_webSocket.State != WebSocketState.Open)
                return;

            await _webSocket.SendAsync(frame, WebSocketMessageType.Binary, endOfMessage: true, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    public async Task ShutdownAsync()
    {
        foreach (var kv in Streams.ToArray())
        {
            if (Streams.TryRemove(kv.Key, out var tcp))
                tcp.Dispose();
        }

        if (_webSocket.State == WebSocketState.Open || _webSocket.State == WebSocketState.CloseReceived)
        {
            try
            {
                await _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Replaced", CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch
            {
                // ignore
            }
        }
    }
}
