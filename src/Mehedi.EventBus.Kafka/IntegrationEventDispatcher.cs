using Microsoft.Extensions.DependencyInjection;

namespace Mehedi.EventBus.Kafka;

/// <summary>What happened to a dispatched message.</summary>
public enum DispatchOutcome
{
    /// <summary>The event was deserialized and every registered handler completed.</summary>
    Handled,

    /// <summary>No handler is registered for the event name; the message is skipped.</summary>
    Ignored
}

/// <summary>
/// Turns a raw message (event name + JSON payload) into a typed event and runs its handlers in a fresh
/// DI scope. Independent of Kafka, so it can be unit tested and reused by other transports.
/// </summary>
public sealed class IntegrationEventDispatcher(
    IServiceScopeFactory scopeFactory,
    IntegrationEventHandlerRegistry registry)
{
    /// <summary>Dispatches one message.</summary>
    /// <exception cref="System.Text.Json.JsonException">The payload cannot be deserialized (a poison message).</exception>
    /// <remarks>Exceptions thrown by handlers propagate to the caller, which decides whether to retry.</remarks>
    public async Task<DispatchOutcome> DispatchAsync(string? eventName, string payload, CancellationToken cancellationToken)
    {
        if (eventName is null || !registry.TryGet(eventName, out var registration))
        {
            return DispatchOutcome.Ignored;
        }

        var @event = IntegrationEventSerializer.Deserialize(payload, registration.EventType);

        await using var scope = scopeFactory.CreateAsyncScope();
        await registration.InvokeHandlersAsync(scope.ServiceProvider, @event, cancellationToken).ConfigureAwait(false);

        return DispatchOutcome.Handled;
    }
}
