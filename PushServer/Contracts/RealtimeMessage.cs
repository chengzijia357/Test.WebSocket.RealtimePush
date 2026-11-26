using System.Text.Json;
using System.Text.Json.Serialization;

namespace PushServer.Contracts;

/// <summary>统一消息信封：type 决定路由，channel 决定订阅归属，seq 用于去重/补推。</summary>
public sealed record RealtimeMessage(
    [property: JsonPropertyName("type")] string Type,
    [property: JsonPropertyName("channel")] string? Channel,
    [property: JsonPropertyName("seq")] long Seq,
    [property: JsonPropertyName("ts")] DateTimeOffset Ts,
    [property: JsonPropertyName("payload")] JsonElement? Payload)
{
    public static RealtimeMessage Create(string type, string? channel, object? payload)
    {
        var element = payload is null
            ? (JsonElement?)null
            : JsonSerializer.SerializeToElement(payload, JsonOptions.Default);

        return new RealtimeMessage(type, channel, 0, DateTimeOffset.UtcNow, element);
    }
}