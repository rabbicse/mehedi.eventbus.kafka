using System.Text.Json;

namespace Mehedi.EventBus.Kafka;

/// <summary>The JSON settings shared by the producer and the consumer.</summary>
public static class IntegrationEventSerializer
{
    /// <summary>camelCase on the wire; property names are matched case-insensitively when reading.</summary>
    public static JsonSerializerOptions Options { get; } = new(JsonSerializerDefaults.Web);

    /// <summary>Serializes an event using its runtime type, so derived properties are included.</summary>
    public static string Serialize(IntegrationEvent @event)
    {
        ArgumentNullException.ThrowIfNull(@event);
        return JsonSerializer.Serialize(@event, @event.GetType(), Options);
    }

    /// <summary>Deserializes a payload into <paramref name="eventType"/>.</summary>
    /// <exception cref="JsonException">The payload is not valid JSON for the type, or is <c>null</c>.</exception>
    public static IntegrationEvent Deserialize(string payload, Type eventType) =>
        JsonSerializer.Deserialize(payload, eventType, Options) as IntegrationEvent
        ?? throw new JsonException($"The payload deserialized to null for '{eventType.Name}'.");

    /// <summary>
    /// Works out the event name of a message: the <see cref="KafkaHeaders.EventName"/> header when present,
    /// otherwise the class name taken from the legacy assembly-qualified <see cref="KafkaHeaders.ClrType"/> header
    /// (which matches the default name of an event without <see cref="IntegrationEventNameAttribute"/>).
    /// </summary>
    public static string? ResolveEventName(string? eventNameHeader, string? clrTypeHeader)
    {
        if (!string.IsNullOrWhiteSpace(eventNameHeader))
        {
            return eventNameHeader;
        }

        if (string.IsNullOrWhiteSpace(clrTypeHeader))
        {
            return null;
        }

        // "Namespace.Outer+CustomerCreatedEvent, Assembly, Version=..." -> "CustomerCreatedEvent"
        var fullName = clrTypeHeader.Split(',', 2)[0].Trim();
        var lastSeparator = fullName.LastIndexOfAny(['.', '+']);
        return lastSeparator >= 0 ? fullName[(lastSeparator + 1)..] : fullName;
    }
}
