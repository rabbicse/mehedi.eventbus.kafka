using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Text;

namespace Mehedi.EventBus.Kafka;

/// <summary>
/// Represents a Kafka event producer responsible for publishing integration events.
/// Source: https://github.com/mizrael/SuperSafeBank/blob/master/src/SuperSafeBank.Transport.Kafka/EventProducer.cs
/// </summary>
public partial class KafkaEventProducer(
        KafkaProducerConfig config,
        ILogger<KafkaEventProducer> logger) : IDisposable, IEventProducer
{
    private readonly KafkaProducerConfig _config = config;
    private readonly ILogger<KafkaEventProducer> _logger = logger;
    private readonly IProducer<Guid, string> _producer = new ProducerBuilder<Guid, string>(new ProducerConfig
    {
        BootstrapServers = config.KafkaConnectionString,
        ClientId = config.InstanceId,
        Acks = Acks.All,
        // No duplicates or reordering from the producer's own retries.
        EnableIdempotence = true,
        MessageTimeoutMs = config.MessageTimeoutMs,
        SocketTimeoutMs = config.SocketTimeoutMs,
        Debug = config.Debug
    }).SetKeySerializer(new KeySerializer<Guid>())
        .SetErrorHandler((_, e) => LogKafkaError(logger, e.Code, e.Reason))
        .SetLogHandler((_, m) => LogKafka(logger, ToLogLevel(m.Level), m.Message))
        .Build();

    /// <summary>
    /// Publishes the specified integration event asynchronously.
    /// </summary>
    /// <param name="event">The integration event to be published.</param>
    /// <param name="cancellationToken">Optional. The cancellation token.</param>
    /// <returns>True when the broker acknowledged (persisted) the message.</returns>
    /// <remarks>
    /// The message key is <see cref="IntegrationEvent.PartitionKey"/>, so events of one aggregate keep their order
    /// when the event overrides it. Headers carry the stable <see cref="KafkaHeaders.EventName"/> that consumers
    /// route on, plus the legacy assembly-qualified <see cref="KafkaHeaders.ClrType"/>.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="event"/> is null.</exception>
    /// <exception cref="ProduceException{TKey, TValue}">The broker rejected the message or did not answer in time.</exception>
    public async Task<bool> PublishAsync(IntegrationEvent @event, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@event);

        var message = new Message<Guid, string>
        {
            Key = @event.PartitionKey,
            Value = IntegrationEventSerializer.Serialize(@event),
            Headers = new Headers
            {
                { KafkaHeaders.EventId, Encoding.UTF8.GetBytes(@event.Id.ToString()) },
                { KafkaHeaders.Sender, Encoding.UTF8.GetBytes(_config.InstanceId) },
                { KafkaHeaders.ClrType, Encoding.UTF8.GetBytes(@event.GetType().AssemblyQualifiedName!) },
                { KafkaHeaders.EventName, Encoding.UTF8.GetBytes(@event.EventName) },
                { KafkaHeaders.ContentType, "application/json"u8.ToArray() }
            }
        };

        var result = await _producer.ProduceAsync(_config.TopicName, message, cancellationToken).ConfigureAwait(false);
        LogPublished(_logger, @event.EventName, @event.Id, result.TopicPartitionOffset.ToString(), result.Status);
        return result.Status == PersistenceStatus.Persisted;
    }

    /// <summary>
    /// Disposes the resources used by the event producer, waiting briefly for in-flight messages.
    /// </summary>
    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
        GC.SuppressFinalize(this);
    }

    private static LogLevel ToLogLevel(SyslogLevel level) => level switch
    {
        SyslogLevel.Emergency or SyslogLevel.Alert or SyslogLevel.Critical or SyslogLevel.Error => LogLevel.Error,
        SyslogLevel.Warning => LogLevel.Warning,
        SyslogLevel.Notice or SyslogLevel.Info => LogLevel.Information,
        _ => LogLevel.Debug
    };

    [LoggerMessage(Message = "Kafka producer: {KafkaMessage}")]
    private static partial void LogKafka(ILogger logger, LogLevel level, string kafkaMessage);

    [LoggerMessage(Level = LogLevel.Error, Message = "Kafka producer error {Code}: {Reason}")]
    private static partial void LogKafkaError(ILogger logger, ErrorCode code, string reason);

    [LoggerMessage(Level = LogLevel.Information, Message = "Published {EventName} {EventId} to {Position} ({Status})")]
    private static partial void LogPublished(ILogger logger, string eventName, Guid eventId, string position, PersistenceStatus status);
}
