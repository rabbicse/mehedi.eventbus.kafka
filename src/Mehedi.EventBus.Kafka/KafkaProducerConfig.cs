namespace Mehedi.EventBus.Kafka;

/// <summary>Settings of <see cref="KafkaEventProducer"/>.</summary>
public record KafkaProducerConfig
{
    /// <summary>Initializes the settings.</summary>
    /// <param name="kafkaConnectionString">Kafka bootstrap servers, for example <c>localhost:9092</c>.</param>
    /// <param name="topicBaseName">The topic events are published to.</param>
    /// <param name="instanceId">Identifies this producer; written to the <see cref="KafkaHeaders.Sender"/> header.</param>
    public KafkaProducerConfig(string kafkaConnectionString, string topicBaseName, string instanceId)
    {
        if (string.IsNullOrWhiteSpace(kafkaConnectionString))
            throw new ArgumentException("kafkaConnectionString cannot be null or whitespace.", nameof(kafkaConnectionString));
        if (string.IsNullOrWhiteSpace(topicBaseName))
            throw new ArgumentException("topicBaseName cannot be null or whitespace.", nameof(topicBaseName));
        if (string.IsNullOrWhiteSpace(instanceId))
            throw new ArgumentException("instanceId cannot be null or whitespace.", nameof(instanceId));

        KafkaConnectionString = kafkaConnectionString;
        TopicName = topicBaseName;
        InstanceId = instanceId;
    }

    /// <summary>Kafka bootstrap servers.</summary>
    public string KafkaConnectionString { get; }

    /// <summary>The topic events are published to.</summary>
    public string TopicName { get; }

    /// <summary>Identifies this producer.</summary>
    public string InstanceId { get; }

    /// <summary>How long a send may take, including retries. Default 30 seconds.</summary>
    public int MessageTimeoutMs { get; set; } = 30000;

    /// <summary>Network request timeout. Default 300 seconds.</summary>
    public int SocketTimeoutMs { get; set; } = 300000;

    /// <summary>
    /// librdkafka debug contexts, for example <c>"msg,broker"</c>. Off by default; turning it on logs every message.
    /// </summary>
    public string? Debug { get; set; }
}
