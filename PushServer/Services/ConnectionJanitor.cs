using System.Net.WebSockets;
using PushServer.Connections;

namespace PushServer.Services;

/// <summary>清理长时间无活动的僵尸连接（心跳超时兜底）。</summary>
public sealed class ConnectionJanitor(
    IConnectionManager connections,
    ILogger<ConnectionJanitor> logger) : BackgroundService
{
    private static readonly TimeSpan IdleLimit = TimeSpan.FromSeconds(90);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(15));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            var now = DateTimeOffset.UtcNow;
            foreach (var connection in connections.GetAll())
            {
                if (now - connection.LastActivityUtc > IdleLimit)
                {
                    logger.LogWarning("回收空闲连接 conn={ConnId}", connection.Id);
                    _ = connection.CloseAsync(WebSocketCloseStatus.PolicyViolation, "idle timeout");
                    connections.Remove(connection.Id);
                }
            }
        }
    }
}