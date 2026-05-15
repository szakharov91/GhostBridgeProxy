using System.Buffers.Binary;

namespace GhostBridgeProxy.Common;

public readonly record struct TunnelFrame(TunnelMessageType Type, uint StreamId, ReadOnlyMemory<byte> Payload);

public static class TunnelProtocol
{
    public const int HeaderSize = 14;
    public const byte Version = 1;
    public const uint Magic = 0x50425247u; // 'G','B','R','P' little-endian as uint on LE machine

    public static int WriteFrame(Span<byte> destination, TunnelMessageType type, uint streamId, ReadOnlySpan<byte> payload)
    {
        if (destination.Length < HeaderSize + payload.Length)
            throw new ArgumentException("Destination too small.", nameof(destination));

        BinaryPrimitives.WriteUInt32LittleEndian(destination[..4], Magic);
        destination[4] = Version;
        destination[5] = (byte)type;
        BinaryPrimitives.WriteUInt32LittleEndian(destination[6..10], streamId);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[10..14], (uint)payload.Length);
        payload.CopyTo(destination[HeaderSize..]);
        return HeaderSize + payload.Length;
    }

    public static byte[] BuildFrame(TunnelMessageType type, uint streamId, ReadOnlySpan<byte> payload)
    {
        var buf = new byte[HeaderSize + payload.Length];
        WriteFrame(buf, type, streamId, payload);
        return buf;
    }

    public static byte[] BuildRegister(string host, int port)
    {
        var hostBytes = System.Text.Encoding.UTF8.GetBytes(host);
        if (hostBytes.Length > ushort.MaxValue)
            throw new ArgumentException("Host too long.", nameof(host));
        if (port is < 0 or > 65535)
            throw new ArgumentOutOfRangeException(nameof(port));

        var payload = new byte[2 + hostBytes.Length + 2];
        BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(0, 2), (ushort)hostBytes.Length);
        hostBytes.CopyTo(payload.AsSpan(2));
        BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(2 + hostBytes.Length), (ushort)port);
        return BuildFrame(TunnelMessageType.Register, 0, payload);
    }

    public static bool TryParseRegisterPayload(ReadOnlySpan<byte> payload, out string host, out int port)
    {
        host = "";
        port = 0;
        if (payload.Length < 4)
            return false;
        var hostLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[..2]);
        if (payload.Length < 4 + hostLen)
            return false;
        host = System.Text.Encoding.UTF8.GetString(payload.Slice(2, hostLen));
        port = BinaryPrimitives.ReadUInt16LittleEndian(payload.Slice(2 + hostLen, 2));
        return true;
    }

    /// <summary>
    /// Tries to read one complete frame from a contiguous buffer. On success, <paramref name="consumed"/> is bytes to remove from the front.
    /// </summary>
    public static bool TryReadFrame(ReadOnlySpan<byte> buffer, out TunnelFrame frame, out int consumed)
    {
        frame = default;
        consumed = 0;
        if (buffer.Length < HeaderSize)
            return false;

        var magic = BinaryPrimitives.ReadUInt32LittleEndian(buffer[..4]);
        if (magic != Magic)
            throw new InvalidDataException("Invalid tunnel frame magic.");

        var version = buffer[4];
        if (version != Version)
            throw new InvalidDataException($"Unsupported tunnel protocol version: {version}.");

        var type = (TunnelMessageType)buffer[5];
        var streamId = BinaryPrimitives.ReadUInt32LittleEndian(buffer[6..10]);
        var payloadLen = BinaryPrimitives.ReadUInt32LittleEndian(buffer[10..14]);
        if (payloadLen > MaxPayloadLength)
            throw new InvalidDataException("Frame payload too large.");

        var total = HeaderSize + (int)payloadLen;
        if (buffer.Length < total)
            return false;

        var payloadCopy = buffer.Slice(HeaderSize, (int)payloadLen).ToArray();
        frame = new TunnelFrame(type, streamId, payloadCopy);
        consumed = total;
        return true;
    }

    public const int MaxPayloadLength = 256 * 1024;
}

/// <summary>
/// Accumulates WebSocket chunks until a full tunnel frame is available.
/// </summary>
public sealed class TunnelReceiveBuffer
{
    private byte[] _buffer = new byte[16384];
    private int _count;

    public void Append(ReadOnlySpan<byte> data)
    {
        if (data.Length == 0)
            return;
        if (_count + data.Length > _buffer.Length)
        {
            var newSize = Math.Max(_buffer.Length * 2, _count + data.Length);
            Array.Resize(ref _buffer, newSize);
        }
        data.CopyTo(_buffer.AsSpan(_count));
        _count += data.Length;
    }

    /// <summary>
    /// Removes and returns the first complete frame if available.
    /// </summary>
    public bool TryTakeFrame(out TunnelFrame frame)
    {
        var span = _buffer.AsSpan(0, _count);
        if (!TunnelProtocol.TryReadFrame(span, out frame, out var consumed))
            return false;
        span.Slice(consumed).CopyTo(_buffer);
        _count -= consumed;
        return true;
    }
}
