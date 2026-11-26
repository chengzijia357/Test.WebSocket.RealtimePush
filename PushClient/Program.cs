using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

var uri = new Uri("ws://localhost:7000/ws/realtime");
var jsonOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web);

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

var backoff = TimeSpan.FromSeconds(1);
var backoffCeiling = TimeSpan.FromSeconds(30);

while (!cts.Token.IsCancellationRequested)
{
    using var socket = new ClientWebSocket();
    socket.Options.KeepAliveInterval = TimeSpan.FromSeconds(20);
    using var pingCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
    Task? pingTask = null;

    try
    {
        await socket.ConnectAsync(uri, cts.Token);
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] 已连接");
        backoff = TimeSpan.FromSeconds(1);   // 连接成功，重置退避

        // 订阅 market 频道
        await SendAsync(socket, new { type = "subscribe", channel = "market" }, jsonOptions, cts.Token);

        // 心跳（用独立 linked token，随本连接一起取消）
        pingTask = Task.Run(async () =>
        {
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(25));
            while (await timer.WaitForNextTickAsync(pingCts.Token))
                await SendAsync(socket, new { type = "ping" }, jsonOptions, pingCts.Token);
        }, pingCts.Token);

        // 接收循环
        var buffer = new byte[16 * 1024];
        var sb = new StringBuilder();
        while (socket.State is WebSocketState.Open or WebSocketState.CloseReceived &&
               !cts.Token.IsCancellationRequested)
        {
            var result = await socket.ReceiveAsync(buffer, cts.Token);
            if (result.CloseStatus is not null)
            {
                // 响应对端的关闭帧，完成双向关闭握手
                Console.WriteLine($"<- 收到关闭帧 {(int)result.CloseStatus} {result.CloseStatus} reason=\"{result.CloseStatusDescription}\"");
                await socket.CloseOutputAsync(result.CloseStatus.Value, result.CloseStatusDescription, CancellationToken.None);
                Console.WriteLine($"-> 发送关闭帧 {(int)result.CloseStatus} {result.CloseStatus}");
                break;
            }

            sb.Append(Encoding.UTF8.GetString(buffer, 0, result.Count));
            if (!result.EndOfMessage) continue;

            var text = sb.ToString(); sb.Clear();
            using var doc = JsonDocument.Parse(text);
            var type = doc.RootElement.GetProperty("type").GetString();
            if (type == "market.tick")
            {
                var p = doc.RootElement.GetProperty("payload");
                Console.WriteLine($"tick seq={doc.RootElement.GetProperty("seq").GetInt64()} " +
                                  $"{p.GetProperty("symbol").GetString()} {p.GetProperty("price").GetDecimal()}");
            }
            else if (type is "welcome" or "pong")
            {
                Console.WriteLine($"<- {type}");
            }
        }
    }
    catch (OperationCanceledException)
    {
        // Ctrl+C：干净关闭统一在 finally 里做
    }
    catch (Exception ex)
    {
        Console.WriteLine($"连接异常：{ex.Message}");
    }
    finally
    {
        // 仅在用户主动退出（Ctrl+C）时补发关闭帧，让服务端看到双向握手而非粗暴断开；
        // 若只是连接出错要重连，则不补发——socket 直接丢弃即可。
        try
        {
            if (cts.Token.IsCancellationRequested &&
                socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
            {
                Console.WriteLine("-> 发送关闭帧 1000 NormalClosure reason=\"client exit\"");
                await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "client exit", CancellationToken.None);
            }
        }
        catch { /* 服务端可能已不可达 */ }

        // 停掉心跳，避免它在 socket 释放后仍尝试发送
        pingCts.Cancel();
        if (pingTask is not null) await SwallowAsync(pingTask);
    }

    if (cts.Token.IsCancellationRequested) break;

    Console.WriteLine($"将在 {backoff.TotalSeconds:F0}s 后重连…");
    try { await Task.Delay(backoff, cts.Token); }
    catch (OperationCanceledException) { break; }   // 等待重连期间 Ctrl+C

    backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, backoffCeiling.Ticks));
}

static async Task SendAsync(ClientWebSocket socket, object message, JsonSerializerOptions options, CancellationToken ct)
{
    var bytes = JsonSerializer.SerializeToUtf8Bytes(message, options);
    await socket.SendAsync(bytes, WebSocketMessageType.Text, endOfMessage: true, ct);
}

static async Task SwallowAsync(Task task)
{
    try { await task; }
    catch { /* 心跳任务随连接收尾失败，忽略 */ }
}