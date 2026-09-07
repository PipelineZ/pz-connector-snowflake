using Pz.Connector.Snowflake;
using Pz.Connectors.Sdk;

return await PzConnectorHost.RunAsync(args, new SnowflakeConnector()).ConfigureAwait(false);
