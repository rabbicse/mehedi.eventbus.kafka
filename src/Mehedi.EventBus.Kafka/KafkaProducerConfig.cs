namespace Mehedi.EventBus.Kafka;


public record KafkaProducerConfig
{
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

    public string KafkaConnectionString { get; }
    public string TopicName { get; }
    public string InstanceId { get; }
    public int MessageTimeoutMs { get; set; } = 30000; // default 30 secs
    public int SocketTimeoutMs { get; set; } = 300000; // default 30 secs
}

