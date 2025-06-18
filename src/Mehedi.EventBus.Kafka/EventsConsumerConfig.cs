namespace Mehedi.EventBus.Kafka;

public record EventsConsumerConfig
{
    public string KafkaConnectionString { get; }
    public string TopicName { get; }
    public string ConsumerGroup { get; }
    public string InstanceId { get; }

    public EventsConsumerConfig(
        string kafkaConnectionString,
        string topicName,
        string consumerGroup,
        string instanceId)
    {
        KafkaConnectionString = Validate(kafkaConnectionString, nameof(kafkaConnectionString));
        TopicName = Validate(topicName, nameof(topicName));
        ConsumerGroup = Validate(consumerGroup, nameof(consumerGroup));
        InstanceId = Validate(instanceId, nameof(instanceId));
    }

    private static string Validate(string value, string name) =>
        string.IsNullOrWhiteSpace(value)
            ? throw new ArgumentException($"{name} cannot be null or whitespace.", name)
            : value;
}
