using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using PushServer.Contracts;

namespace PushServer.Connections;

public sealed class RealtimeConnection
{
    private const int ReceiveBufferSize = 16 * 1024;
    private const int MaxMessageBytes = 1 * 1024 * 1024;   // 单消息上限 1MB
    private const int OutboundCapacity = 256;

    private readonly WebSocket _socket;
    private readonly ILogger _logger;
    private readonly Channel<RealtimeMessage> _outbound;
    private readonly ConcurrentDictionary<string, byte> _subscriptions = new();
    private readonly TaskCompletionSource _closed =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _writeDone =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private long _seq;
    private long _lastActivityTicks = DateTime.UtcNow.Ticks;
    private int _closing;   // 0/1，防止重复发起关闭

    public string Id { get; } = Guid.NewGuid().ToString("N");
    public DateTimeOffset LastActivityUtc =>
        new(Interlocked.Read(ref _lastActivityTicks), TimeSpan.Zero); // ticks 为 UTC

    /// <summary>关闭握手完成后完成（停机时用于等待优雅关闭）。</summary>
    public Task Closed => _closed.Task;

    public RealtimeConnection(WebSocket socket, ILogger logger)
    {
        _socket = socket;
        _logger = logger;
        _outbound = Channel.CreateBounded<RealtimeMessage>(new BoundedChannelOptions(OutboundCapacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,  // 慢消费者：丢最旧，保新鲜
            SingleReader = true,
            SingleWriter = false
        });
    }

    public void Subscribe(string channel) => _subscriptions.TryAdd(channel, 0);
    public void Unsubscribe(string channel) => _subscriptions.TryRemove(channel, out _);
    public bool IsSubscribed(string channel) => _subscriptions.ContainsKey(channel);

    /// <summary>非阻塞入队；未订阅该频道或队列已满则返回 false。</summary>
    public bool TryEnqueue(RealtimeMessage message)
    {
        if (message.Channel is not null && !IsSubscribed(message.Channel))
            return false;

        return _outbound.Writer.TryWrite(message with { Seq = Interlocked.Increment(ref _seq) });
    }

    /// <summary>请求优雅关闭（停机 / 空闲回收）。先停写出站通道，再发关闭帧；
    /// 关闭握手由读循环在收到对端关闭帧时完成。<see cref="Closed"/> 表示全部收尾。</summary>
    public Task CloseAsync(WebSocketCloseStatus status, string? reason)
    {
        if (Interlocked.Exchange(ref _closing, 1) == 0)
            _ = CloseCoreAsync(status, reason);
        return _closed.Task;
    }

    private async Task CloseCoreAsync(WebSocketCloseStatus status, string? reason)
    {
        try
        {
            // 1) 停写：完成出站通道 → 写循环排空后自行退出。
            //    不能取消 SendAsync 的 token（那会中止 socket），只能靠通道完成来优雅收尾。
            _outbound.Writer.TryComplete();
            await _writeDone.Task;

            // 2) 发关闭帧。CloseOutputAsync 只占用发送侧，不会打断读循环正在进行的
            //    ReceiveAsync；它发出 Close 帧后，对端会回一帧，从而解锁读循环。
            if (_socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
            {
                _logger.LogInformation(
                    "WS 发送关闭帧 conn={ConnId} status={Status}({Code}) reason={Reason}",
                    Id, status, (int)status, reason);
                await _socket.CloseOutputAsync(status, reason, CancellationToken.None);
            }
        }
        catch (Exception ex) when (ex is WebSocketException or OperationCanceledException or ObjectDisposedException)
        {
            // 对端已断开，正常收尾
        }
    }

    /// <summary>读循环 + 写循环并行，任一结束即整体收尾。</summary>
    public async Task RunAsync(CancellationToken ct)
    {
        var read = ReadLoopAsync(ct);
        var write = WriteLoopAsync(ct);
        try
        {
            await Task.WhenAll(read, write);
        }
        finally
        {
            _closed.TrySetResult();
        }
    }

    private async Task ReadLoopAsync(CancellationToken ct)
    {
        var buffer = new byte[ReceiveBufferSize];
        var text = new StringBuilder();

        try
        {
            // 注意：绝不能用「取消 token」来打断接收——取消 ReceiveAsync 会把 socket 置为
            // Aborted，之后就再也发不出关闭帧。这里退出只靠：对端发关闭帧 / 对端断开 /
            // 宿主硬中止（ct，即 RequestAborted）。
            while (_socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
            {
                WebSocketReceiveResult result;
                try
                {
                    result = await _socket.ReceiveAsync(buffer, ct);
                }
                catch (OperationCanceledException)
                {
                    break; // 宿主硬中止，不握手
                }
                catch (WebSocketException ex)
                {
                    _logger.LogWarning(ex, "WS 接收异常 conn={ConnId}", Id);
                    break; // 对端异常断开，无法回复关闭帧
                }

                Interlocked.Exchange(ref _lastActivityTicks, DateTime.UtcNow.Ticks);

                if (result.CloseStatus is not null)
                {
                    _logger.LogInformation(
                        "WS 收到关闭帧 conn={ConnId} status={Status}({Code}) reason={Reason}",
                        Id, result.CloseStatus, (int)result.CloseStatus, result.CloseStatusDescription);

                    // 对端先关：回一帧完成握手（此时 state 为 CloseReceived）。
                    // 若是我方先关（已经过 CloseCoreAsync，state 已从 CloseSent 到 Closed），
                    // 无需再发——此时即握手完成。
                    if (_socket.State == WebSocketState.CloseReceived)
                    {
                        _logger.LogInformation(
                            "WS 发送关闭帧 conn={ConnId} status={Status}({Code}) reason={Reason}",
                            Id, result.CloseStatus, (int)result.CloseStatus, result.CloseStatusDescription);
                        await _socket.CloseOutputAsync(result.CloseStatus.Value, result.CloseStatusDescription, CancellationToken.None);
                    }
                    else
                    {
                        _logger.LogInformation("WS 关闭握手完成 conn={ConnId}", Id);
                    }
                    break;
                }
                if (result.MessageType == WebSocketMessageType.Close) break;

                text.Append(Encoding.UTF8.GetString(buffer, 0, result.Count));
                if (!result.EndOfMessage)                       // 处理分片
                {
                    if (text.Length > MaxMessageBytes) { text.Clear(); break; }
                    continue;
                }

                var payload = text.ToString();
                text.Clear();
                if (payload.Length is > 0 and <= MaxMessageBytes)
                    await HandleTextAsync(payload, ct);
            }
        }
        finally
        {
            _outbound.Writer.TryComplete();  // 停写循环
        }
    }

    private async Task HandleTextAsync(string json, CancellationToken ct)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            var type = root.TryGetProperty("type", out var t) ? t.GetString() : null;
            var channel = root.TryGetProperty("channel", out var c) ? c.GetString() : null;

            switch (type)
            {
                case "subscribe":
                    if (channel is not null)
                    {
                        Subscribe(channel);
                        await ReplyAsync("welcome", channel, new { subscribed = channel, connId = Id }, ct);
                    }
                    break;

                case "unsubscribe":
                    if (channel is not null) Unsubscribe(channel);
                    break;

                case "ping":
                    await ReplyAsync("pong", null, new { serverTime = DateTimeOffset.UtcNow }, ct);
                    break;

                default:
                    await ReplyAsync("error", null, new { code = "unknown_type", type }, ct);
                    break;
            }
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "WS 消息解析失败 conn={ConnId}", Id);
            await ReplyAsync("error", null, new { code = "bad_json" }, ct);
        }
    }

    private ValueTask ReplyAsync(string type, string? channel, object payload, CancellationToken ct)
    {
        var msg = RealtimeMessage.Create(type, channel, payload);
        TryEnqueue(msg);
        return ValueTask.CompletedTask;
    }

    private async Task WriteLoopAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var message in _outbound.Reader.ReadAllAsync(ct))
            {
                if (_socket.State is not WebSocketState.Open)
                    break; // 关闭中，不再发送

                var bytes = JsonSerializer.SerializeToUtf8Bytes(message, JsonOptions.Default);
                await _socket.SendAsync(bytes, WebSocketMessageType.Text, endOfMessage: true, ct);
            }
        }
        catch (OperationCanceledException) { /* 宿主硬中止 */ }
        catch (WebSocketException ex)
        {
            _logger.LogWarning(ex, "WS 发送异常 conn={ConnId}", Id);
        }
        finally
        {
            _outbound.Writer.TryComplete();
            _writeDone.TrySetResult();
        }
    }
}