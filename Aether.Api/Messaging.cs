using System.Text.Json;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

public sealed record OrderCreated(Guid EventId, Guid OrderId);

public static class Broker
{
    public const string Queue = "inventory.reserve";
    public static ConnectionFactory Factory(IConfiguration config) => new()
    {
        Uri = new Uri(config["RABBITMQ_URL"] ?? "amqp://guest:guest@localhost:5672/"),
        AutomaticRecoveryEnabled = true
    };
    public static async Task DeclareAsync(IChannel channel, CancellationToken ct)
    {
        await channel.QueueDeclareAsync("inventory.reserve.dlq", durable: true, exclusive: false, autoDelete: false, cancellationToken: ct);
        await channel.QueueDeclareAsync(Queue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum", ["x-delivery-limit"] = 5,
                ["x-dead-letter-exchange"] = "", ["x-dead-letter-routing-key"] = "inventory.reserve.dlq" }, cancellationToken: ct);
    }
}

public sealed class OutboxPublisher(OrderStore store, IConfiguration config, ILogger<OutboxPublisher> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var conn = await Broker.Factory(config).CreateConnectionAsync(ct);
                await using var channel = await conn.CreateChannelAsync(new CreateChannelOptions(
                    publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true), ct);
                await Broker.DeclareAsync(channel, ct);
                while (!ct.IsCancellationRequested)
                {
                    await using var db = await store.DataSource.OpenConnectionAsync(ct);
                    await using var tx = await db.BeginTransactionAsync(ct);
                    await using var select = new NpgsqlCommand("SELECT event_id,order_id FROM outbox WHERE published_at IS NULL ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED", db, tx);
                    Guid eventId, orderId;
                    await using (var r = await select.ExecuteReaderAsync(ct))
                    {
                        if (!await r.ReadAsync(ct)) { await tx.RollbackAsync(ct); await Task.Delay(250, ct); continue; }
                        eventId = r.GetGuid(0); orderId = r.GetGuid(1);
                    }
                    var props = new BasicProperties { Persistent = true, ContentType = "application/json", MessageId = eventId.ToString() };
                    await channel.BasicPublishAsync("", Broker.Queue, mandatory: true, basicProperties: props,
                        body: JsonSerializer.SerializeToUtf8Bytes(new OrderCreated(eventId, orderId)), cancellationToken: ct);
                    await using var mark = new NpgsqlCommand("UPDATE outbox SET published_at=now() WHERE event_id=@id", db, tx);
                    mark.Parameters.AddWithValue("id", eventId);
                    await mark.ExecuteNonQueryAsync(ct);
                    await tx.CommitAsync(ct);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex) { logger.LogWarning(ex, "Publisher reconnecting"); await Task.Delay(2000, ct); }
        }
    }
}

public sealed class InventoryConsumer(OrderStore store, IConfiguration config, ILogger<InventoryConsumer> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var conn = await Broker.Factory(config).CreateConnectionAsync(ct);
                await using var channel = await conn.CreateChannelAsync(cancellationToken: ct);
                await Broker.DeclareAsync(channel, ct);
                await channel.BasicQosAsync(0, 8, false, ct);
                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += async (_, delivery) =>
                {
                    OrderCreated? message;
                    try { message = JsonSerializer.Deserialize<OrderCreated>(delivery.Body.Span); }
                    catch (JsonException) { await channel.BasicRejectAsync(delivery.DeliveryTag, false, ct); return; }
                    if (message is null || message.EventId == Guid.Empty || message.OrderId == Guid.Empty)
                    { await channel.BasicRejectAsync(delivery.DeliveryTag, false, ct); return; }
                    try
                    {
                        await store.ProcessAsync(message.EventId, message.OrderId, ct);
                        await channel.BasicAckAsync(delivery.DeliveryTag, false, ct);
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "Order event {EventId} failed; requeueing", message.EventId);
                        await Task.Delay(1000, ct);
                        await channel.BasicNackAsync(delivery.DeliveryTag, false, true, ct);
                    }
                };
                await channel.BasicConsumeAsync(Broker.Queue, autoAck: false, consumer, ct);
                while (channel.IsOpen && !ct.IsCancellationRequested) await Task.Delay(1000, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex) { logger.LogWarning(ex, "Consumer reconnecting"); await Task.Delay(2000, ct); }
        }
    }
}
