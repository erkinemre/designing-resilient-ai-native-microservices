public sealed record ForecastResult(double PredictedOrdersPerMinute, double NaiveMae, double ModelMae,
    int PredictedBacklog, int RecommendedReplicaFloor, string Mode, int TrainingSamples);

public sealed class ForecastService(OrderStore store)
{
    public async Task<ForecastResult> PredictAsync(CancellationToken ct)
    {
        var counts = await store.MinuteCountsAsync(121, ct);
        return PredictFromCounts(counts);
    }

    public static ForecastResult PredictFromCounts(double[] counts)
    {
        if (counts.Length < 5) throw new ArgumentException("At least five minute buckets are required.");
        // Use only complete minutes. Fit an AR(3) ridge model on rolling observations.
        var history = counts[..^1];
        var samples = new List<(double[] X, double Y)>();
        for (var i = 3; i < history.Length; i++)
            samples.Add(([1, history[i - 1], history[i - 2], history[i - 3]], history[i]));
        var active = history.Count(v => v > 0);
        var naive = Math.Max(0, history[^1]);
        if (samples.Count < 20 || active < 10)
            return new(naive, 0, 0, 0, 1, "insufficient-data", samples.Count);

        var split = Math.Max(10, (int)(samples.Count * .8));
        var train = samples.Take(split).ToArray();
        var validation = samples.Skip(split).ToArray();
        var weights = Fit(train);
        double modelError = validation.Average(s => Math.Abs(s.Y - Math.Max(0, Dot(weights, s.X))));
        double naiveError = validation.Average(s => Math.Abs(s.Y - s.X[1]));
        var trusted = modelError < naiveError;
        var forecast = trusted ? Math.Max(0, Dot(weights, [1, history[^1], history[^2], history[^3]])) : naive;
        // Two-minute warm-up horizon, 20 orders/minute per replica (illustrative capacity).
        var backlog = trusted ? Math.Max(0, (int)Math.Ceiling(forecast * 2 - 20)) : 0;
        var floor = trusted ? Math.Clamp((int)Math.Ceiling(forecast / 20), 1, 10) : 1;
        return new(Math.Round(forecast, 2), Math.Round(naiveError, 2), Math.Round(modelError, 2),
            backlog, floor, trusted ? "model" : "naive-fallback", samples.Count);
    }

    private static double Dot(double[] a, double[] b) => a.Zip(b, (x, y) => x * y).Sum();

    private static double[] Fit((double[] X, double Y)[] samples)
    {
        const int n = 4;
        var matrix = new double[n, n + 1];
        for (var i = 0; i < n; i++)
        {
            for (var j = 0; j < n; j++)
                matrix[i, j] = samples.Sum(s => s.X[i] * s.X[j]) + (i == j ? 1 : 0);
            matrix[i, n] = samples.Sum(s => s.X[i] * s.Y);
        }
        for (var col = 0; col < n; col++)
        {
            var pivot = Enumerable.Range(col, n - col).MaxBy(row => Math.Abs(matrix[row, col]));
            for (var j = col; j <= n; j++) (matrix[col, j], matrix[pivot, j]) = (matrix[pivot, j], matrix[col, j]);
            var divisor = matrix[col, col];
            for (var j = col; j <= n; j++) matrix[col, j] /= divisor;
            for (var row = 0; row < n; row++)
            {
                if (row == col) continue;
                var scale = matrix[row, col];
                for (var j = col; j <= n; j++) matrix[row, j] -= scale * matrix[col, j];
            }
        }
        return Enumerable.Range(0, n).Select(i => matrix[i, n]).ToArray();
    }
}
