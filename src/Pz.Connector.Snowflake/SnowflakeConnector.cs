using Snowflake.Data.Client;
using Pz.Connectors.Abstractions;


namespace Pz.Connector.Snowflake;

/// <summary>Snowflake source + sink connector, served out of process through Pz.Connectors.Sdk.
/// Key-pair (JWT) authentication only -- no password auth surface. Registered under the logical name
/// "snowflake". Connection options: account/user/private_key_path/database/warehouse required;
/// private_key_passphrase/role optional. <c>base_dir</c> is never written by a user: the package
/// manifest declares a project-directory anchor, so pz injects the project directory under that key
/// and a relative <c>private_key_path</c> resolves against the project rather than against the
/// working directory of a connector process the user never launched.</summary>
public sealed class SnowflakeConnector : ISourceConnector, ISinkConnector
{
    public ConnectorInfo Info => new("snowflake", "0.1.0", ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities => ConnectorCapabilities.ColumnPruning |
        ConnectorCapabilities.PredicatePushdown | ConnectorCapabilities.Merge |
        ConnectorCapabilities.Transactional | ConnectorCapabilities.ReplaceWrites |
        ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound;

    public string ConnectionConfigSchema =>
        """{ "type": "object", "required": ["account","user","private_key_path","database","warehouse"], "properties": { "account": { "type": "string" }, "user": { "type": "string" }, "private_key_path": { "type": "string" }, "private_key_passphrase": { "type": "string" }, "database": { "type": "string" }, "warehouse": { "type": "string" }, "role": { "type": "string" }, "base_dir": { "type": "string" } }, "additionalProperties": false }""";

    // "columns" is not read by SnowflakeSource today (its schema is always resolved from the
    // driver's own reported metadata via SfTypeMap, never a declared contract), but the
    // bounded-window trio (initial/max_window/until) needs a columns: contract to make its bounds
    // computable before the first extraction (PZ0213) -- and ConnectorConfigValidator.MergeColumns
    // folds a dataset's declared columns: into these same options ahead of validation, so without
    // this property that contract would itself fail PZ0301, leaving the declared BoundedWindow
    // capability unreachable.
    public string DatasetConfigSchema =>
        """{ "type": "object", "properties": { "query": { "type": "string" }, "columns": { "type": "object", "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } } }, "additionalProperties": false }""";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);

    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        try
        {
            await using var connection = new SnowflakeDbConnection { ConnectionString = BuildConnectionString(config) };
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = "select 1";
            await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
            return new ConnectionCheck(true);
        }
        catch (Exception ex)
        {
            // ConnectionCheck carries no transience field; fold it into the message tag so callers
            // can parse it (same convention as the other database connector checks).
            return new ConnectionCheck(false, $"{(SfErrors.IsTransient(ex) ? "transient" : "permanent")}: {ex.Message}");
        }
    }

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new SnowflakeSource(BuildConnectionString(config)));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new SnowflakeSink(BuildConnectionString(config)));

    /// <summary>Builds the Snowflake.Data connection string via the driver's own
    /// <see cref="SnowflakeDbConnectionStringBuilder"/> (a plain <see cref="System.Data.Common.DbConnectionStringBuilder"/>
    /// subclass) rather than hand-rolled <c>key=value;</c> concatenation -- a value containing a
    /// <c>;</c> (a passphrase, for instance) would otherwise silently truncate and spawn a spurious
    /// property. Key-pair (JWT) auth only. Public (not internal) so CheckConnectionAsync's convention
    /// and Tasks 6-7's OpenAsync implementations share one credential-resolution path.</summary>
    public static string BuildConnectionString(ConnectorConfig config)
    {
        string Require(string key) => config.GetString(key) ??
            throw new PzConnectorException($"snowflake connection requires '{key}'", isTransient: false);

        var builder = new SnowflakeDbConnectionStringBuilder
        {
            ["account"] = Require("account"),
            ["user"] = Require("user"),
            ["authenticator"] = "snowflake_jwt",
            ["private_key_file"] = ResolvePrivateKeyPath(config),
            ["db"] = Require("database"),
            ["warehouse"] = Require("warehouse"),
            ["application"] = "pz",
        };
        if (config.GetString("private_key_passphrase") is { } pwd) { builder["private_key_pwd"] = pwd; }
        if (config.GetString("role") is { } role) { builder["role"] = role; }
        return builder.ConnectionString;
    }

    /// <summary>A relative <c>private_key_path</c> is anchored on <c>base_dir</c> when pz supplies
    /// one; an absolute path, or a relative one with no anchor, is handed to the driver as written.</summary>
    internal static string ResolvePrivateKeyPath(ConnectorConfig config)
    {
        var path = config.GetString("private_key_path") ??
            throw new PzConnectorException("snowflake connection requires 'private_key_path'", isTransient: false);
        if (Path.IsPathRooted(path) || config.GetString("base_dir") is not { Length: > 0 } baseDir)
        {
            return path;
        }

        return Path.GetFullPath(Path.Combine(baseDir, path));
    }
}
