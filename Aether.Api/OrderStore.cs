using Npgsql;

public sealed class OrderStore(NpgsqlDataSource db)
{
    public async Task InitializeAsync(CancellationToken ct)
    {
        await using var cmd = db.CreateCommand("""
            CREATE TABLE IF NOT EXISTS orders (
              id uuid PRIMARY KEY, sku text NOT NULL, quantity integer NOT NULL,
              status text NOT NULL, created_at timestamptz NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox (
              event_id uuid PRIMARY KEY, order_id uuid NOT NULL REFERENCES orders(id),
              created_at timestamptz NOT NULL, published_at timestamptz);
            CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox (created_at) WHERE published_at IS NULL;
            CREATE TABLE IF NOT EXISTS inbox (
              consumer text NOT NULL, event_id uuid NOT NULL,
              processed_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (consumer, event_id));
            """);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<Order> CreateAsync(string sku, int quantity, CancellationToken ct)
    {
        var order = new Order(Guid.NewGuid(), sku, quantity, "Pending", DateTimeOffset.UtcNow);
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await using (var cmd = new NpgsqlCommand("INSERT INTO orders VALUES (@id,@sku,@quantity,@status,@created)", conn, tx))
        {
            cmd.Parameters.AddWithValue("id", order.Id);
            cmd.Parameters.AddWithValue("sku", order.Sku);
            cmd.Parameters.AddWithValue("quantity", order.Quantity);
            cmd.Parameters.AddWithValue("status", order.Status);
            cmd.Parameters.AddWithValue("created", order.CreatedAt);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        await using (var cmd = new NpgsqlCommand("INSERT INTO outbox(event_id,order_id,created_at) VALUES (@event,@order,@created)", conn, tx))
        {
            cmd.Parameters.AddWithValue("event", Guid.NewGuid());
            cmd.Parameters.AddWithValue("order", order.Id);
            cmd.Parameters.AddWithValue("created", order.CreatedAt);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        await tx.CommitAsync(ct);
        return order;
    }

    public async Task<Order?> GetAsync(Guid id, CancellationToken ct)
    {
        await using var cmd = db.CreateCommand("SELECT id,sku,quantity,status,created_at FROM orders WHERE id=@id");
        cmd.Parameters.AddWithValue("id", id);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        return await r.ReadAsync(ct) ? ReadOrder(r) : null;
    }

    public async Task<List<Order>> ListAsync(CancellationToken ct)
    {
        await using var cmd = db.CreateCommand("SELECT id,sku,quantity,status,created_at FROM orders ORDER BY created_at DESC LIMIT 100");
        await using var r = await cmd.ExecuteReaderAsync(ct);
        var result = new List<Order>();
        while (await r.ReadAsync(ct)) result.Add(ReadOrder(r));
        return result;
    }

    private static Order ReadOrder(NpgsqlDataReader r) => new(r.GetGuid(0), r.GetString(1), r.GetInt32(2),
        r.GetString(3), r.GetFieldValue<DateTimeOffset>(4));

    public async Task<Stats> GetStatsAsync(CancellationToken ct)
    {
        await using var cmd = db.CreateCommand("SELECT (SELECT count(*) FROM orders),(SELECT count(*) FROM outbox WHERE published_at IS NULL),(SELECT count(*) FROM inbox)");
        await using var r = await cmd.ExecuteReaderAsync(ct);
        await r.ReadAsync(ct);
        return new Stats(r.GetInt64(0), r.GetInt64(1), r.GetInt64(2));
    }

    public async Task ProcessAsync(Guid eventId, Guid orderId, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await using var receipt = new NpgsqlCommand("INSERT INTO inbox(consumer,event_id) VALUES ('inventory',@event) ON CONFLICT DO NOTHING", conn, tx);
        receipt.Parameters.AddWithValue("event", eventId);
        if (await receipt.ExecuteNonQueryAsync(ct) == 1)
        {
            await using var update = new NpgsqlCommand("UPDATE orders SET status='InventoryReserved' WHERE id=@id", conn, tx);
            update.Parameters.AddWithValue("id", orderId);
            if (await update.ExecuteNonQueryAsync(ct) != 1) throw new InvalidOperationException("Order not found.");
        }
        await tx.CommitAsync(ct);
    }

    public async Task<double[]> MinuteCountsAsync(int minutes, CancellationToken ct)
    {
        await using var cmd = db.CreateCommand("SELECT date_trunc('minute',created_at),count(*) FROM orders WHERE created_at >= now() - (@minutes * interval '1 minute') GROUP BY 1");
        cmd.Parameters.AddWithValue("minutes", minutes);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        var buckets = new Dictionary<DateTime, double>();
        while (await r.ReadAsync(ct)) buckets[r.GetDateTime(0)] = r.GetInt64(1);
        var now = DateTime.UtcNow;
        var start = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc).AddMinutes(1 - minutes);
        return Enumerable.Range(0, minutes).Select(i => buckets.GetValueOrDefault(start.AddMinutes(i))).ToArray();
    }

    public NpgsqlDataSource DataSource => db;
}

public sealed class SchemaInitializer(OrderStore store, ILogger<SchemaInitializer> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            try { await store.InitializeAsync(ct); return; }
            catch (Exception ex) when (attempt < 30 && !ct.IsCancellationRequested)
            { logger.LogWarning(ex, "Database unavailable; retry {Attempt}", attempt); await Task.Delay(2000, ct); }
        }
    }
    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}
