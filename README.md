# Test.WebSocket.RealtimePush

基于 .NET 10 的 WebSocket 实时推送示例：ASP.NET Core Minimal API + 原生 `System.Net.WebSockets`，演示服务端主动推送、双向交互、背压控制、心跳与优雅停机。

## 环境与技术选型

- .NET 10（LTS，2025-11-11 发布，支持至 2028-11），`TargetFramework` 均为 `net10.0`
- ASP.NET Core Minimal API + 原生 `System.Net.WebSockets`
- 支持 .NET 10 新增的 `WebSocketStream`（把 WebSocket 抽象成真正的 Stream，可与 `StreamReader`、`JsonSerializer`、`PipeReader/PipeWriter` 等流式生态协作）

## 项目结构

```
Test.WebSocket.RealtimePush.sln
├─ PushServer                 # 服务端（ASP.NET Core）
│  ├─ Program.cs
│  ├─ Contracts/RealtimeMessage.cs
│  ├─ Contracts/JsonOptions.cs
│  ├─ Connections/RealtimeConnection.cs
│  ├─ Connections/ConnectionManager.cs
│  ├─ Services/MarketTickerService.cs
│  └─ Services/ConnectionJanitor.cs
└─ PushClient                 # .NET 客户端（控制台）
   └─ Program.cs
```

## 消息契约

统一消息信封：`type` 决定路由，`channel` 决定订阅归属，`seq` 用于去重/补推。

```json
{ "type": "market.tick", "channel": "market", "seq": 1, "ts": "…", "payload": { … } }
```

支持的消息类型：

| type | 方向 | 说明 |
| --- | --- | --- |
| `subscribe` | 客户端 → 服务端 | 订阅频道，服务端回 `welcome` |
| `unsubscribe` | 客户端 → 服务端 | 退订频道 |
| `ping` | 客户端 → 服务端 | 心跳，服务端回 `pong` |
| `welcome` / `pong` / `error` | 服务端 → 客户端 | 应答 |
| `market.tick` | 服务端 → 客户端 | 行情推送 |

## 核心特性

- **服务端主动推**：`MarketTickerService` 每秒向 `market` 频道推送一条 tick，业务侧只需 `SendToChannelAsync`，无需客户端轮询。
- **双向交互**：同一条连接完成 `subscribe/ping/ack`，替代一整套轮询接口。
- **低延迟与低开销**：握手后仅帧头开销，消息经有界通道直达写循环。
- **稳健性**：分片重组、消息大小限制（1MB）、心跳与空闲回收（90s）、指数退避重连（1s→2s→4s…封顶 30s）、优雅停机通知。
- **背压可控**：每连接 `Channel` 有界（256）+ `DropOldest`，慢消费者不会拖垮服务端内存。
- **可运维**：连接注册表 + `/health` 暴露连接数，日志带 `connId`。

## 运行

```bash
# 终端 1：启动服务端
dotnet run --project PushServer --urls http://localhost:6000

# 终端 2：启动客户端
dotnet run --project PushClient

# 终端 3：观察连接数
curl http://localhost:6000/health
```

预期现象：

- 客户端连接后每秒打印一条 `market.tick`
- `/health` 的 `connections` 随连接数变化
- 杀掉服务端后客户端按 1s→2s→4s 退避重连
- 服务端停机时客户端收到 1001/1012 类关闭码

## 浏览器端验证

只需十几行 JavaScript：

```javascript
const ws = new WebSocket("ws://localhost:6000/ws/realtime");
ws.onopen = () => ws.send(JSON.stringify({ type: "subscribe", channel: "market" }));
ws.onmessage = (e) => {
  const msg = JSON.parse(e.data);
  if (msg.type === "market.tick") console.log(msg.seq, msg.payload.symbol, msg.payload.price);
};
ws.onclose = (e) => console.log("closed", e.code, e.reason);
```

## 关键实现说明

- **读写分离**：`RealtimeConnection` 中读循环与写循环并行，任一结束即整体收尾。
- **背压**：每连接 `Channel.CreateBounded` + `FullMode = DropOldest`。
- **优雅停机**：`ApplicationStopping` 注册回调，向所有连接发送 1011（EndpointUnavailable）关闭码。
- **慢消费者丢弃**：队列满时丢最旧消息，保持数据新鲜。

> .NET 10 的 `WebSocketStream` 可将 WebSocket 从消息帧模型提升为流模型（`CreateReadableMessageStream` / `CreateWritableMessageStream` / `Create`），特别适合高频实时服务与协议隧道场景。本示例主循环保留经典 `ReceiveAsync/SendAsync` 以清晰展示分片与关闭语义。