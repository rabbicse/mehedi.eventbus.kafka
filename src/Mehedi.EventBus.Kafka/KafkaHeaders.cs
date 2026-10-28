namespace Mehedi.EventBus.Kafka;

/// <summary>Names of the Kafka message headers written and read by this package.</summary>
public static class KafkaHeaders
{
    /// <summary>The integration event id.</summary>
    public const string EventId = "id";

    /// <summary>The <see cref="KafkaProducerConfig.InstanceId"/> of the producer that sent the message.</summary>
    public const string Sender = "sender";

    /// <summary>The assembly-qualified .NET type of the event (kept for consumers older than 1.1).</summary>
    public const string ClrType = "type";

    /// <summary>The stable event name (<see cref="IntegrationEvent.EventName"/>). Consumers route on this.</summary>
    public const string EventName = "event-name";

    /// <summary>The payload format, always <c>application/json</c>.</summary>
    public const string ContentType = "content-type";

    /// <summary>Dead-letter header: why handling failed.</summary>
    public const string DeadLetterReason = "dlq-reason";

    /// <summary>Dead-letter header: the topic the message was consumed from.</summary>
    public const string DeadLetterOriginalTopic = "dlq-original-topic";

    /// <summary>Dead-letter header: the partition the message was consumed from.</summary>
    public const string DeadLetterOriginalPartition = "dlq-original-partition";

    /// <summary>Dead-letter header: the offset the message was consumed from.</summary>
    public const string DeadLetterOriginalOffset = "dlq-original-offset";
}
