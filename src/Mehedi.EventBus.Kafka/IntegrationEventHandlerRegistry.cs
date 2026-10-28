using Microsoft.Extensions.DependencyInjection;

namespace Mehedi.EventBus.Kafka;

/// <summary>
/// Maps event names to the .NET contract types a consumer understands, and knows how to invoke the
/// registered <see cref="IIntegrationEventHandler{TEvent}"/>s for each.
/// </summary>
public sealed class IntegrationEventHandlerRegistry
{
    private readonly Dictionary<string, Registration> _registrations = new(StringComparer.Ordinal);

    /// <summary>A registered event: its contract type and a typed invoker for its handlers.</summary>
    /// <param name="EventType">The contract type the payload is deserialized into.</param>
    /// <param name="InvokeHandlersAsync">Resolves every handler of the event from the scope and calls it in turn.</param>
    public sealed record Registration(
        Type EventType,
        Func<IServiceProvider, IntegrationEvent, CancellationToken, Task<int>> InvokeHandlersAsync);

    /// <summary>The event names this registry routes.</summary>
    public IReadOnlyCollection<string> EventNames => _registrations.Keys;

    /// <summary>Registers <typeparamref name="TEvent"/> under its <see cref="IntegrationEvent.EventName"/>.</summary>
    /// <exception cref="InvalidOperationException">A different type is already registered under the same name.</exception>
    public void Register<TEvent>() where TEvent : IntegrationEvent
    {
        var name = IntegrationEventName.Of<TEvent>();

        if (_registrations.TryGetValue(name, out var existing))
        {
            if (existing.EventType != typeof(TEvent))
            {
                throw new InvalidOperationException(
                    $"Event name '{name}' is already registered for {existing.EventType.FullName}; " +
                    $"{typeof(TEvent).FullName} cannot use it too.");
            }

            return;
        }

        _registrations[name] = new Registration(typeof(TEvent), InvokeAsync<TEvent>);
    }

    /// <summary>Finds the registration for an event name.</summary>
    public bool TryGet(string eventName, out Registration registration) =>
        _registrations.TryGetValue(eventName, out registration!);

    private static async Task<int> InvokeAsync<TEvent>(
        IServiceProvider services, IntegrationEvent @event, CancellationToken cancellationToken)
        where TEvent : IntegrationEvent
    {
        var handled = 0;
        foreach (var handler in services.GetServices<IIntegrationEventHandler<TEvent>>())
        {
            await handler.HandleAsync((TEvent)@event, cancellationToken).ConfigureAwait(false);
            handled++;
        }

        return handled;
    }
}
