using PushServer.Connections;
using PushServer.Contracts;

namespace PushServer.Services;

/// <summary>模拟行情源：每秒向 market 频道推送一条 tick，演示"服务端主动推"。</summary>
public sealed class MarketTickerService(
    IConnectionManager connections,
    ILogger<MarketTickerService> logger) : BackgroundService
{
    private static readonly string[] Symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
    private readonly Random _random = new();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                var symbol = Symbols[_random.Next(Symbols.Length)];
                var price = 60_000 + _random.NextDouble() * 5_000;

                var message = RealtimeMessage.Create(
                    "market.tick", "market",
                    new { symbol, price = Math.Round(price, 2), volume = _random.Next(100, 5_000) });

                await connections.SendToChannelAsync("market", message, stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "行情推送失败");
            }
        }
    }
}