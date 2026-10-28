using Confluent.Kafka;

namespace Mehedi.EventBus.Kafka;

/// <summary>Settings of a hosted Kafka consumer registered with <c>AddKafkaConsumer</c>.</summary>
public sealed class KafkaConsumerOptions
{
    /// <summary>Kafka bootstrap servers, for example <c>localhost:9092</c>. Required.</summary>
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>
    /// Consumer group. Every instance of a service uses the same group, so each message is handled once per service. Required.
    /// </summary>
    public string GroupId { get; set; } = string.Empty;

    /// <summary>Topics to subscribe to. At least one is required.</summary>
    public IList<string> Topics { get; } = [];

    /// <summary>
    /// When set, messages whose <see cref="KafkaHeaders.Sender"/> header equals this value are skipped
    /// (a service that publishes and consumes the same topic does not handle its own events).
    /// </summary>
    public string? SkipMessagesFromSender { get; set; }

    /// <summary>How many times a failing handler is retried before the message is dead-lettered. Default 3.</summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>Delay before the first retry; retry <c>n</c> waits <c>n × RetryDelay</c>. Default 1 second.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Suffix of the dead-letter topic (<c>orders</c> → <c>orders.dlq</c>). Default <c>.dlq</c>.
    /// Set to <c>null</c> or empty to drop failed messages (they are still logged) instead.
    /// </summary>
    public string? DeadLetterTopicSuffix { get; set; } = ".dlq";

    /// <summary>Where a new consumer group starts reading. Default <see cref="AutoOffsetReset.Earliest"/>.</summary>
    public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Earliest;

    /// <summary>Optional hook to set any other librdkafka consumer property (security, timeouts, ...).</summary>
    public Action<ConsumerConfig>? ConfigureConsumer { get; set; }

    /// <summary>Optional hook to configure the producer used for dead-lettering.</summary>
    public Action<ProducerConfig>? ConfigureDeadLetterProducer { get; set; }

    /// <summary>Throws when a required setting is missing or out of range.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(BootstrapServers))
        {
            throw new InvalidOperationException($"{nameof(KafkaConsumerOptions)}.{nameof(BootstrapServers)} is required.");
        }

        if (string.IsNullOrWhiteSpace(GroupId))
        {
            throw new InvalidOperationException($"{nameof(KafkaConsumerOptions)}.{nameof(GroupId)} is required.");
        }

        if (Topics.Count == 0 || Topics.Any(string.IsNullOrWhiteSpace))
        {
            throw new InvalidOperationException($"{nameof(KafkaConsumerOptions)}.{nameof(Topics)} needs at least one non-empty topic.");
        }

        if (MaxRetryAttempts < 0)
        {
            throw new InvalidOperationException($"{nameof(KafkaConsumerOptions)}.{nameof(MaxRetryAttempts)} cannot be negative.");
        }

        if (RetryDelay < TimeSpan.Zero)
        {
            throw new InvalidOperationException($"{nameof(KafkaConsumerOptions)}.{nameof(RetryDelay)} cannot be negative.");
        }
    }
}
