using System.Net.WebSockets;
using PushServer.Connections;
using PushServer.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<IConnectionManager, ConnectionManager>();
builder.Services.AddHostedService<MarketTickerService>();
builder.Services.AddHostedService<ConnectionJanitor>();

var app = builder.Build();

// WebSocket 中间件：心跳 + Origin 白名单（生产环境务必收紧）
app.UseWebSockets(new WebSocketOptions
{
    KeepAliveInterval = TimeSpan.FromSeconds(30),
    AllowedOrigins = { "https://localhost:7001", "http://localhost:7000" }
});

// 实时推送端点
app.Map("/ws/realtime", async (HttpContext context, IConnectionManager manager) =>
{
    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = StatusCodes.Status400BadRequest;
        await context.Response.WriteAsync("WebSocket request expected.");
        return;
    }

    using var socket = await context.WebSockets.AcceptWebSocketAsync();
    var connection = manager.Create(socket);

    try
    {
        // 关键：await 读循环，保持中间件管道在连接期间存活
        await connection.RunAsync(context.RequestAborted);
    }
    finally
    {
        manager.Remove(connection.Id);
    }
});

// 诊断接口
app.MapGet("/health", (IConnectionManager manager) => Results.Ok(new
{
    status = "ok",
    connections = manager.Count,
    time = DateTimeOffset.UtcNow
}));

// TEMP 测试端点：触发与停机相同的关闭流程，便于观察关闭握手日志
app.MapGet("/shutdown", async (IConnectionManager manager, IHostApplicationLifetime lifetime) =>
{
    await manager.CloseAllAsync(WebSocketCloseStatus.EndpointUnavailable, "server shutting down");
    return Results.Ok("connections closed");
});

// 优雅停机：通知客户端切换实例
app.Lifetime.ApplicationStopping.Register(() =>
{
    var manager = app.Services.GetRequiredService<IConnectionManager>();
    manager.CloseAllAsync(WebSocketCloseStatus.EndpointUnavailable, "server shutting down")
           .GetAwaiter().GetResult();
});

app.Run();