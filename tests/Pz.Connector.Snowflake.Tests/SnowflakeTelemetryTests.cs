using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Sdk;

namespace Pz.Connector.Snowflake.Tests;

public class SnowflakeTelemetryTests
{
    private static readonly Schema OneCol = new([new Field("id", Int64Type.Default, nullable: true)], null);

    private static RemoteCalls Remote() => new(new ActivitySource("pz.test.snowflake"));

    private static async Task<SnowflakeSinkWriteSession> SessionWithOneSpoolFileAsync(
        string mode, Func<string, CancellationToken, Task> execute)
    {
        var spec = new OutputSpec("sf", "PUBLIC.T", mode, "fail_on_change", new Dictionary<string, object?>()) { Keys = ["id"] };
        var spoolDir = Path.Combine(Path.GetTempPath(), "pz-snowflake-test", Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(spoolDir);
        var session = new SnowflakeSinkWriteSession("account=a", spec, OneCol, "PUBLIC", "T", spoolDir, Remote(), execute);
        using var batch = new RecordBatch(OneCol, [new Int64Array.Builder().Append(1).Build()], 1);
        await session.WriteBatchAsync(batch, CancellationToken.None);
        return session;
    }

    [Theory]
    [InlineData("append")]
    [InlineData("replace")]
    [InlineData("merge")]
    public async Task Load_counts_one_request_and_one_statement_extra_per_statement(string mode)
    {
        var issued = new List<string>();
        await using var session = await SessionWithOneSpoolFileAsync(mode, (sql, _) => { issued.Add(sql); return Task.CompletedTask; });

        var acc = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(acc))
        {
            await session.LoadStagedAsync("\"PUBLIC\".\"s\"", "\"PUBLIC\".\"t\"", CancellationToken.None);
        }

        // create stage, one PUT, create staging table, COPY, the target statement.
        Assert.Equal(5, issued.Count);
        var stats = acc.Snapshot();
        Assert.Equal(5, stats.Requests);
        Assert.Contains(new KeyValuePair<string, double>("snowflake.statements", 5), stats.Extras);
    }

    [Fact]
    public async Task Failed_statement_still_counts_and_is_wrapped_with_its_phase()
    {
        await using var session = await SessionWithOneSpoolFileAsync(
            "append", (sql, _) => sql.StartsWith("put", StringComparison.OrdinalIgnoreCase)
                ? Task.FromException(new IOException("boom"))
                : Task.CompletedTask);

        var acc = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(acc))
        {
            var ex = await Assert.ThrowsAsync<PzConnectorException>(
                () => session.LoadStagedAsync("\"PUBLIC\".\"s\"", "\"PUBLIC\".\"t\"", CancellationToken.None));
            Assert.Contains("stage upload", ex.Message);
            Assert.True(ex.IsTransient);
        }

        Assert.Equal(2, acc.Snapshot().Requests); // create stage + the failed PUT
    }

    [Fact]
    public async Task Statement_helper_counts_once_and_rethrows_failures()
    {
        var acc = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(acc))
        {
            Assert.Equal(7, await SfStatement.RunAsync(Remote(), () => Task.FromResult(7)));
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => SfStatement.RunAsync<int>(Remote(), () => throw new InvalidOperationException("x")));
        }

        var stats = acc.Snapshot();
        Assert.Equal(2, stats.Requests);
        Assert.Contains(new KeyValuePair<string, double>("snowflake.statements", 2), stats.Extras);
    }

    [Fact]
    public void Connector_declares_ReportsStats()
    {
        Assert.True(new SnowflakeConnector(Remote()).Capabilities.HasFlag(ConnectorCapabilities.ReportsStats));
    }
}
