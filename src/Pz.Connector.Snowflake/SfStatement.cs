using Pz.Connectors.Sdk;

namespace Pz.Connector.Snowflake;

/// <summary>The one place a Snowflake statement is counted. <c>RemoteCall.Dispose</c> counts the
/// request, so callers must not also add requests for the same statement; the
/// <c>snowflake.statements</c> extra is a separate number that stays meaningful if a request ever
/// spans more than one statement.</summary>
internal static class SfStatement
{
    public static async Task<T> RunAsync<T>(RemoteCalls remote, Func<Task<T>> run)
    {
        using var call = remote.Start("statement").Extra("snowflake.statements", 1);
        try
        {
            return await run().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            call.Fail(ex);
            throw;
        }
    }

    public static Task RunAsync(RemoteCalls remote, Func<Task> run) =>
        RunAsync<bool>(remote, async () =>
        {
            await run().ConfigureAwait(false);
            return true;
        });
}
