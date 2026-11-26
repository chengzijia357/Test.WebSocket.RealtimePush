using System.Collections.Concurrent;
using System.Net.WebSockets;
using PushServer.Contracts;

namespace PushServer.Connections;

public interface IConnectionManager
{
    RealtimeConnection Create(WebSocket socket);
    void Remove(string connectionId);
    int Count { get; }
    IReadOnlyCollection<RealtimeConnection> GetAll();
    ValueTask BroadcastAsync(RealtimeMessage message, CancellationToken ct);
    ValueTask SendToChannelAsync(string channel, RealtimeMessage message, CancellationToken ct);
    Task CloseAllAsync(WebSocketCloseStatus status, string reason);
}

public sealed class ConnectionManager(ILogger<ConnectionManager> logger) : IConnectionManager
{
    private readonly ConcurrentDictionary<string, RealtimeConnection> _connections = new();

    public int Count => _connections.Count;

    public IReadOnlyCollection<RealtimeConnection> GetAll() =>
        _connections.Values.ToList(); // 快照

    public RealtimeConnection Create(WebSocket socket)
    {
        var connection = new RealtimeConnection(socket, logger);
        _connections[connection.Id] = connection;
        logger.LogInformation("WS 已连接 conn={ConnId} 当前连接数={Count}", connection.Id, Count);
        return connection;
    }

    public void Remove(string connectionId) => _connections.TryRemove(connectionId, out _);

    public async ValueTask BroadcastAsync(RealtimeMessage message, CancellationToken ct)
    {
        foreach (var connection in _connections.Values)
            connection.TryEnqueue(message);          // 非阻塞：队列满按 DropOldest 处理
        await ValueTask.CompletedTask;
    }

    public async ValueTask SendToChannelAsync(string channel, RealtimeMessage message, CancellationToken ct)
    {
        foreach (var connection in _connections.Values)
        {
            if (connection.IsSubscribed(channel))
                connection.TryEnqueue(message);
        }
        await ValueTask.CompletedTask;
    }

    public async Task CloseAllAsync(WebSocketCloseStatus status, string reason)
    {
        // 请求每个连接关闭，并等待它们完成关闭握手。
        var tasks = _connections.Values
            .Select(c => c.CloseAsync(status, reason))
            .ToArray();
        _connections.Clear();

        // 对端不回帧由读循环内的 5s 超时兜底，这里再加一层保险避免停机卡死。
        try { await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(6)); }
        catch { /* 超时 / 对端已断开，停机收尾不阻塞 */ }
    }
}