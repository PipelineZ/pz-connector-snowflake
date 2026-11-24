using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connector.Snowflake.Tests;

public class SnowflakeNestedTests
{
    private static Schema NestedSchema() => new([
        new Field("id", Int64Type.Default, nullable: true),
        new Field("tags", new ListType(new Field("item", StringType.Default, true)), nullable: true),
        new Field("owner", new StructType([new Field("name", StringType.Default, true)]), nullable: true)], null);

    private static OutputSpec Spec(string mode, params string[] keys) =>
        new("sf", "PUBLIC.T", mode, "fail_on_change", new Dictionary<string, object?>()) { Keys = keys };

    private static RecordBatch Batch()
    {
        var builder = new ArrowBatchBuilder(NestedSchema());
        builder.AppendRow([1L, new List<object?> { "a", "b\"q" }, new Dictionary<string, object?> { ["name"] = "x" }]);
        builder.AppendRow([2L, null, null]);
        return builder.Flush()!;
    }

    [Fact]
    public void Sink_declares_nested_types() =>
        Assert.True(new SnowflakeConnector().Capabilities.HasFlag(ConnectorCapabilities.NestedTypes));

    [Fact]
    public void Dataset_schema_does_not_restrict_column_types_to_the_flat_names()
    {
        var schema = new SnowflakeConnector().DatasetConfigSchema;
        Assert.DoesNotContain("\"enum\"", schema);
        Assert.Contains("\"minLength\": 1", schema);
    }

    [Fact]
    public void Nested_columns_are_variant_in_ddl()
    {
        Assert.Equal("VARIANT", SfTypeMap.ToSnowflakeDdl(NestedSchema().FieldsList[1].DataType));
        Assert.Equal("VARIANT", SfTypeMap.ToInformationSchemaDisplay(NestedSchema().FieldsList[2].DataType));
        Assert.Equal(
            "create table if not exists \"S\".\"T\" (\"id\" BIGINT, \"tags\" VARIANT, \"owner\" VARIANT)",
            SfDdl.BuildCreateTableSql("S", "T", NestedSchema()));
        Assert.Contains("\"tags\" VARIANT",
            SfDdl.BuildCreateStagingTableSql("\"S\".\"x\"", NestedSchema(), includeSequenceColumn: false));
    }

    [Fact]
    public void Copy_parses_nested_cells_with_parse_json_and_keeps_the_sequence_column_raw()
    {
        var sql = SfDdl.BuildCopyIntoStagingSql("\"S\".\"l\"", "\"S\".\"st\"", NestedSchema(), includeSequenceColumn: true);
        Assert.Contains("(select $1, parse_json($2), parse_json($3), $4 from @\"S\".\"st\")", sql);
    }

    [Fact]
    public void Csv_writes_nested_cells_as_quoted_json_and_nulls_as_the_null_token()
    {
        using var batch = Batch();
        var writer = new StringWriter();
        SfCsv.WriteBatch(batch, writer);
        var lines = writer.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal("1,\"[\"\"a\"\",\"\"b\\u0022q\"\"]\",\"{\"\"name\"\":\"\"x\"\"}\"", lines[0]);
        Assert.Equal("2,\\N,\\N", lines[1]);
    }

    [Fact]
    public async Task Merge_key_that_is_nested_is_refused_naming_the_column()
    {
        var sink = new SnowflakeSink("account=a");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Spec("merge", "tags"), NestedSchema(), CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("tags", ex.Message);
        Assert.Contains("a merge key must be a flat value", ex.Message);
    }

    [Fact]
    public async Task Flat_merge_key_with_nested_value_columns_is_accepted()
    {
        var sink = new SnowflakeSink("account=a");
        await using var session = await sink.BeginWriteAsync(Spec("merge", "id"), NestedSchema(), CancellationToken.None);
        Assert.NotNull(session);
    }

    [Fact]
    public async Task Reads_semi_structured_columns_as_json_text_without_a_contract()
    {
        var reader = new FakeDbDataReader(
            [("v", "VARIANT", typeof(string)), ("a", "ARRAY", typeof(string)), ("o", "OBJECT", typeof(string))],
            [["{\"k\":1}", "[1,2]", "{\"z\":true}"]]);
        var batches = new List<RecordBatch>();
        await foreach (var b in SnowflakeArrowReader.ReadBatchesAsync(reader, BatchOptions.Default, CancellationToken.None))
        {
            batches.Add(b);
        }

        var batch = Assert.Single(batches);
        Assert.Equal("[1,2]", ((StringArray)batch.Column("a")).GetString(0));
        batch.Dispose();
    }

    [Fact]
    public async Task Contract_declared_nested_type_parses_the_json()
    {
        var reader = new FakeDbDataReader(
            [("id", "FIXED", typeof(long)), ("tags", "ARRAY", typeof(string)), ("owner", "OBJECT", typeof(string))],
            [[1L, "[\"a\",\"b\"]", "{\"name\":\"x\"}"], [2L, null, null]]);
        var nested = SnowflakeArrowReader.NestedContract(new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string>
            {
                ["id"] = "bigint", ["tags"] = "varchar[]", ["owner"] = "struct(name varchar)",
            },
        });
        Assert.Equal(2, nested.Count);

        var batches = new List<RecordBatch>();
        await foreach (var b in SnowflakeArrowReader.ReadBatchesAsync(
            reader, BatchOptions.Default, CancellationToken.None, nested))
        {
            batches.Add(b);
        }

        var batch = Assert.Single(batches);
        Assert.Equal(2, batch.Length);
        Assert.IsType<ListType>(batch.Schema.GetFieldByName("tags").DataType);
        Assert.Equal("[\"a\",\"b\"]", ArrowCell.ToJson(batch.Column("tags"), 0));
        Assert.Equal("{\"name\":\"x\"}", ArrowCell.ToJson(batch.Column("owner"), 0));
        Assert.True(batch.Column("tags").IsNull(1));
        batch.Dispose();
    }

    [Fact]
    public async Task Contract_mismatch_names_the_column_and_not_the_value()
    {
        var reader = new FakeDbDataReader([("tags", "ARRAY", typeof(string))], [["{\"secret\":1}"]]);
        var nested = SnowflakeArrowReader.NestedContract(new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["tags"] = "varchar[]" },
        });
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var _ in SnowflakeArrowReader.ReadBatchesAsync(
                reader, BatchOptions.Default, CancellationToken.None, nested)) { }
        });
        Assert.Contains("tags", ex.Message);
        Assert.DoesNotContain("secret", ex.Message);
    }

    [Fact]
    public void Contract_nested_type_on_a_numeric_column_is_refused()
    {
        var reader = new FakeDbDataReader([("n", "FIXED", typeof(long))], []);
        var nested = SnowflakeArrowReader.NestedContract(new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["n"] = "int[]" },
        });
        var ex = Assert.Throws<PzConnectorException>(() => SnowflakeArrowReader.BuildSchema(reader, "dataset 'x'", nested));
        Assert.Contains("'n'", ex.Message);
    }
}
