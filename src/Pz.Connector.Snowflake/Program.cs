using Pz.Connector.Snowflake;
using Pz.Connectors.Sdk;

return await PzConnectorHost.RunAsync(args, ctx => new SnowflakeConnector(ctx.Remote)).ConfigureAwait(false);
