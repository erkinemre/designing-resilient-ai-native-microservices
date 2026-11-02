using Npgsql;

if (args.Contains("--forecast-self-test"))
{
    var series = Enumerable.Range(0, 121).Select(i => i % 5 == 0 ? 30.0 : 10.0).ToArray();
    var result = ForecastService.PredictFromCounts(series);
    var fallback = ForecastService.PredictFromCounts(new double[121]);
    if (double.IsNaN(result.PredictedOrdersPerMinute) || result.TrainingSamples < 100 || result.RecommendedReplicaFloor is < 1 or > 10)
        throw new InvalidOperationException("Forecast self-test failed.");
    if (fallback.Mode != "insufficient-data" || fallback.PredictedBacklog != 0)
        throw new InvalidOperationException("Forecast fallback self-test failed.");
    Console.WriteLine($"Forecast self-test passed: mode={result.Mode}, MAE={result.ModelMae}, forecast={result.PredictedOrdersPerMinute}.");
    return;
}

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton(new NpgsqlDataSourceBuilder(builder.Configuration.GetConnectionString("Postgres")
    ?? throw new InvalidOperationException("ConnectionStrings:Postgres is required.")).Build());
builder.Services.AddSingleton<OrderStore>();
builder.Services.AddSingleton<ForecastService>();
builder.Services.AddHostedService<SchemaInitializer>();
if (builder.Configuration["ROLE"] == "worker")
{
    builder.Services.AddHostedService<OutboxPublisher>();
    builder.Services.AddHostedService<InventoryConsumer>();
}

var app = builder.Build();
app.MapGet("/health", () => Results.Ok(new { status = "ok", role = app.Configuration["ROLE"] ?? "api" }));
app.MapGet("/ready", async (OrderStore store, CancellationToken ct) =>
{
    await store.GetStatsAsync(ct);
    return Results.Ok(new { status = "ready" });
});
app.MapPost("/orders", async (CreateOrder request, OrderStore store, CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(request.Sku) || request.Quantity is < 1 or > 1000)
        return Results.BadRequest(new { error = "Sku is required and quantity must be 1..1000." });
    var order = await store.CreateAsync(request.Sku.Trim(), request.Quantity, ct);
    return Results.Created($"/orders/{order.Id}", order);
});
app.MapGet("/orders/{id:guid}", async (Guid id, OrderStore store, CancellationToken ct) =>
    await store.GetAsync(id, ct) is { } order ? Results.Ok(order) : Results.NotFound());
app.MapGet("/orders", async (OrderStore store, CancellationToken ct) => Results.Ok(await store.ListAsync(ct)));
app.MapGet("/advisor", async (ForecastService forecast, CancellationToken ct) => Results.Ok(await forecast.PredictAsync(ct)));
app.MapGet("/metrics", async (ForecastService forecast, OrderStore store, CancellationToken ct) =>
{
    var advice = await forecast.PredictAsync(ct);
    var stats = await store.GetStatsAsync(ct);
    var body = $"# TYPE aether_predicted_backlog gauge\naether_predicted_backlog {advice.PredictedBacklog}\n" +
        $"# TYPE aether_outbox_pending gauge\naether_outbox_pending {stats.OutboxPending}\n" +
        $"# TYPE aether_orders_total gauge\naether_orders_total {stats.Orders}\n" +
        $"# TYPE aether_processed_total gauge\naether_processed_total {stats.Processed}\n";
    return Results.Text(body, "text/plain; version=0.0.4");
});
app.Run();

public record CreateOrder(string Sku, int Quantity);
public record Order(Guid Id, string Sku, int Quantity, string Status, DateTimeOffset CreatedAt);
public record Stats(long Orders, long OutboxPending, long Processed);
