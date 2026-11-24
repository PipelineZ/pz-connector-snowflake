using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Abstractions.Formats;
using Pz.Connectors.Abstractions.Memory;

namespace Pz.Connector.Snowflake;

/// <summary>Typed DbDataReader → Arrow streaming: per-column compiled appenders bind Snowflake.Data's
/// typed getters directly to Arrow builders — no object[] row, no boxing. Buffers come from
/// PooledNativeAllocator.Shared — pooled off-heap native memory, so steady-state ingest never puts a
/// batch on the LOH; builders are reused across batches via Clear(). Columns are appended in
/// ascending ordinal order every row, which is what SequentialAccess requires. Byte accounting matches
/// the engine's conventions: fixed widths, utf8 = 4 + UTF-8 bytes, +1 validity bit (0.125) per value.
///
/// Snowflake.Data's typed getters diverge from SqlClient's: FIXED scale-0 always materializes as
/// `long` regardless of declared precision (narrow columns are read via a checked long→int cast, not
/// GetInt32), and DATE materializes as `DateTime`, not `DateOnly`. TIMESTAMP_NTZ/_LTZ come back as
/// `DateTime`; TIMESTAMP_TZ comes back as `DateTimeOffset` — both resolve to the same Arrow
/// TimestampType, so the appender is chosen from the reader's reported CLR type, not the type name.</summary>
internal static class SnowflakeArrowReader
{
    /// <summary>The nested types a dataset's <c>columns:</c> contract declares, by column name. Only a
    /// nested declaration changes a read; a flat one is ignored (the driver's own metadata types it).
    /// Option values arrive as a string map or, from a generic caller, an object map.</summary>
    public static IReadOnlyDictionary<string, IArrowType> NestedContract(IReadOnlyDictionary<string, object?> options)
    {
        var result = new Dictionary<string, IArrowType>(StringComparer.Ordinal);
        if (!options.TryGetValue("columns", out var raw))
        {
            return result;
        }

        IEnumerable<KeyValuePair<string, string?>> declared = raw switch
        {
            IReadOnlyDictionary<string, string> m => m.Select(kv => new KeyValuePair<string, string?>(kv.Key, kv.Value)),
            IReadOnlyDictionary<string, object?> m => m.Select(kv => new KeyValuePair<string, string?>(kv.Key, kv.Value?.ToString())),
            _ => [],
        };
        foreach (var (name, text) in declared)
        {
            if (text is not null && ColumnContractType.IsNested(text))
            {
                result[name] = ColumnContractType.ToArrowType(text, name);
            }
        }

        return result;
    }

    public static Schema BuildSchema(DbDataReader reader, string subject,
        IReadOnlyDictionary<string, IArrowType>? nested = null)
    {
        var fields = new List<Field>(reader.FieldCount);
        foreach (var column in reader.GetColumnSchema())
        {
            var typeName = column.DataTypeName ?? "<unknown>";
            if (nested is not null && nested.TryGetValue(column.ColumnName, out var declared))
            {
                if (!SfTypeMap.IsSemiStructured(typeName) &&
                    !string.Equals(typeName, "TEXT", StringComparison.OrdinalIgnoreCase) &&
                    !string.Equals(typeName, "VARCHAR", StringComparison.OrdinalIgnoreCase))
                {
                    throw new PzConnectorException(
                        $"'{subject}': column '{column.ColumnName}' has Snowflake type '{typeName}' and cannot " +
                        "be read as a nested type -- hint: declare a nested columns: type only for an " +
                        "ARRAY, OBJECT, MAP or VARIANT column (or JSON text in a VARCHAR)",
                        isTransient: false);
                }

                fields.Add(new Field(column.ColumnName, declared, nullable: true));
                continue;
            }

            var (precision, scale) = ResolvePrecisionScale(column);
            if (!SfTypeMap.TryResolve(typeName, precision, scale, out var arrowType))
            {
                throw new PzConnectorException(
                    $"'{subject}': column '{column.ColumnName}' has Snowflake type '{typeName}', which is " +
                    "outside the supported matrix -- hint: cast it in query: (e.g. col::varchar) or exclude " +
                    "it via columns:",
                    isTransient: false);
            }

            fields.Add(new Field(column.ColumnName, arrowType!, nullable: true));
        }

        return new Schema(fields, null);
    }

    /// <summary>The driver always reports NumericPrecision on FIXED/NUMBER columns; the null branch
    /// here only serves test doubles that don't populate DbColumn's numeric metadata. The CLR type
    /// disambiguates which FIXED width was meant: `long` covers scale-0 columns up to 18 digits (the
    /// max Int64Type range SfTypeMap recognizes), `decimal` falls back to the same 38-precision,
    /// 9-scale default the sink's overflow message quotes.</summary>
    private static (short Precision, short Scale) ResolvePrecisionScale(DbColumn column)
    {
        if (column.NumericPrecision.HasValue)
        {
            return ((short)column.NumericPrecision.Value, (short)(column.NumericScale ?? 0));
        }

        if (column.DataType == typeof(long))
        {
            return (18, 0);
        }

        if (column.DataType == typeof(decimal))
        {
            return (38, 9);
        }

        return (0, 0);
    }

    public static async IAsyncEnumerable<RecordBatch> ReadBatchesAsync(
        DbDataReader reader, BatchOptions options, [EnumeratorCancellation] CancellationToken ct = default,
        IReadOnlyDictionary<string, IArrowType>? nested = null)
    {
        var schema = BuildSchema(reader, "read", nested);
        var plans = BuildColumnPlans(reader, schema);
        var pendingRows = 0;
        var bytesEstimate = 0d;

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            for (var i = 0; i < plans.Length; i++)
            {
                bytesEstimate += plans[i].Append() + 0.125d;
            }

            pendingRows++;
            if (bytesEstimate >= options.TargetBatchBytes || pendingRows >= options.MaxRowsPerBatch)
            {
                ct.ThrowIfCancellationRequested();
                yield return Build(schema, plans, pendingRows);
                pendingRows = 0;
                bytesEstimate = 0;
            }
        }

        if (pendingRows > 0)
        {
            ct.ThrowIfCancellationRequested();
            yield return Build(schema, plans, pendingRows);
        }
    }

    private static RecordBatch Build(Schema schema, ColumnPlan[] plans, int rows)
    {
        var arrays = new IArrowArray[plans.Length];
        for (var i = 0; i < plans.Length; i++)
        {
            arrays[i] = plans[i].BuildAndReset();
        }

        return new RecordBatch(schema, arrays, rows);
    }

    /// <summary>One column's hot-path pair: Append() reads the current row's value (typed getter →
    /// typed builder append; null check first) and returns the value's estimated byte width;
    /// BuildAndReset() finishes the Arrow array from pooled memory and clears the builder for reuse.</summary>
    private sealed record ColumnPlan(Func<double> Append, Func<IArrowArray> BuildAndReset);

    private static ColumnPlan[] BuildColumnPlans(DbDataReader reader, Schema schema)
    {
        var allocator = PooledNativeAllocator.Shared;
        var columns = reader.GetColumnSchema();
        var plans = new ColumnPlan[columns.Count];
        for (var i = 0; i < columns.Count; i++)
        {
            var ordinal = i;
            var name = columns[i].ColumnName;
            var clrType = columns[i].DataType;
            plans[i] = schema.FieldsList[i].DataType switch
            {
                NestedType nestedType => PlanNested(schema.FieldsList[i], nestedType, reader, ordinal),
                Int32Type => Plan<Int32Array, Int32Array.Builder>(new Int32Array.Builder(), allocator,
                    b => b.Append(checked((int)reader.GetFieldValue<long>(ordinal))), _ => 4d, reader, ordinal),
                Int64Type => Plan<Int64Array, Int64Array.Builder>(new Int64Array.Builder(), allocator,
                    b => b.Append(reader.GetFieldValue<long>(ordinal)), _ => 8d, reader, ordinal),
                DoubleType => Plan<DoubleArray, DoubleArray.Builder>(new DoubleArray.Builder(), allocator,
                    b => b.Append(reader.GetFieldValue<double>(ordinal)), _ => 8d, reader, ordinal),
                Decimal128Type dec => Plan<Decimal128Array, Decimal128Array.Builder>(
                    new Decimal128Array.Builder(dec), allocator,
                    b =>
                    {
                        try
                        {
                            b.Append(reader.GetFieldValue<decimal>(ordinal));
                        }
                        catch (OverflowException ex)
                        {
                            throw new PzConnectorException(
                                $"column '{name}': value exceeds decimal128({dec.Precision},{dec.Scale}) scale -- " +
                                "cast the column in query: (e.g. col::number(38,9)) or declare it varchar in columns:",
                                isTransient: false, innerException: ex);
                        }
                    }, _ => 16d, reader, ordinal),
                StringType => PlanUtf8(allocator, reader, ordinal, r => r.GetFieldValue<string>(ordinal)),
                BooleanType => Plan<BooleanArray, BooleanArray.Builder>(new BooleanArray.Builder(), allocator,
                    b => b.Append(reader.GetFieldValue<bool>(ordinal)), _ => 0.125d, reader, ordinal),
                Date32Type => Plan<Date32Array, Date32Array.Builder>(new Date32Array.Builder(), allocator,
                    b => b.Append(DateOnly.FromDateTime(reader.GetDateTime(ordinal))), _ => 4d, reader, ordinal),
                TimestampType ts when clrType == typeof(DateTimeOffset) =>
                    Plan<TimestampArray, TimestampArray.Builder>(new TimestampArray.Builder(ts), allocator,
                        b => b.Append(reader.GetFieldValue<DateTimeOffset>(ordinal).ToUniversalTime()),
                        _ => 8d, reader, ordinal),
                TimestampType ts => Plan<TimestampArray, TimestampArray.Builder>(new TimestampArray.Builder(ts), allocator,
                    b => b.Append(new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc))),
                    _ => 8d, reader, ordinal),
                _ => throw new InvalidOperationException("unreachable"),
            };
        }

        return plans;
    }

    private static ColumnPlan Plan<TArray, TBuilder>(
        TBuilder builder, Apache.Arrow.Memory.MemoryAllocator allocator,
        Action<TBuilder> appendValue, Func<TBuilder, double> width,
        DbDataReader reader, int ordinal)
        where TArray : IArrowArray
        where TBuilder : class, IArrowArrayBuilder<TArray, TBuilder> =>
        new(
            Append: () =>
            {
                if (reader.IsDBNull(ordinal))
                {
                    builder.AppendNull();
                    return 0d;
                }

                appendValue(builder);
                return width(builder);
            },
            BuildAndReset: () =>
            {
                var array = builder.Build(allocator);
                builder.Clear();
                return array;
            });

    /// <summary>A contract-declared nested column: the cell's JSON text is parsed against the declared
    /// type and appended through a one-column <see cref="ArrowBatchBuilder"/>. A cell that does not fit
    /// the declared type fails naming the column, never echoing the value.</summary>
    private static ColumnPlan PlanNested(Field field, NestedType type, DbDataReader reader, int ordinal)
    {
        var builder = new ArrowBatchBuilder(new Schema([field], null), int.MaxValue, maxRowsPerBatch: int.MaxValue);
        return new ColumnPlan(
            Append: () =>
            {
                if (reader.IsDBNull(ordinal))
                {
                    builder.AppendRow([null]);
                    return 0d;
                }

                var json = reader.GetFieldValue<string>(ordinal);
                object? value;
                try
                {
                    value = ColumnContractType.ValueFromJson(json, type);
                }
                catch (Exception ex) when (ex is InvalidOperationException or FormatException or System.Text.Json.JsonException)
                {
                    throw new PzConnectorException(
                        $"column '{field.Name}': a value does not match the declared type '{type.Name}' " +
                        "-- hint: fix the type in columns:, or cast the column to varchar in query:",
                        isTransient: false, innerException: ex);
                }

                builder.AppendRow([value]);
                return 4d + Encoding.UTF8.GetByteCount(json);
            },
            BuildAndReset: () => builder.Flush()!.Column(0));
    }

    private static ColumnPlan PlanUtf8(
        Apache.Arrow.Memory.MemoryAllocator allocator, DbDataReader reader, int ordinal,
        Func<DbDataReader, string> read)
    {
        var builder = new StringArray.Builder();
        return new ColumnPlan(
            Append: () =>
            {
                if (reader.IsDBNull(ordinal))
                {
                    builder.AppendNull();
                    return 0d;
                }

                var value = read(reader);
                builder.Append(value);
                return 4d + Encoding.UTF8.GetByteCount(value);
            },
            BuildAndReset: () =>
            {
                var array = builder.Build(allocator);
                builder.Clear();
                return array;
            });
    }
}
