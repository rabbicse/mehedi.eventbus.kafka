using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Text;
using System.Text.Json;

namespace Mehedi.EventBus.Kafka;

/// <summary>
/// Represents a Kafka event producer responsible for publishing integration events.
/// Source: https://github.com/mizrael/SuperSafeBank/blob/master/src/SuperSafeBank.Transport.Kafka/EventProducer.cs
/// </summary>
public class EventProducer(
        KafkaProducerConfig config,
        ILogger<EventProducer> logger) : IDisposable, IEventProducer
{
    private readonly KafkaProducerConfig _config = config;
    private readonly ILogger<EventProducer> _logger = logger;
    private IProducer<Guid, string> _producer = new ProducerBuilder<Guid, string>(new ProducerConfig
    {
        BootstrapServers = config.KafkaConnectionString,
        Acks = Acks.All,
        MessageTimeoutMs = config.MessageTimeoutMs,
        SocketTimeoutMs = config.SocketTimeoutMs,
        Debug = "msg,broker,protocol"
    }).SetKeySerializer(new KeySerializer<Guid>())
        .SetErrorHandler((_, e) => logger.LogError("Producer error: {Reason}", e.Reason))
        .SetLogHandler((_, m) => logger.Log(
                m.Level switch { SyslogLevel.Error => LogLevel.Error, _ => LogLevel.Information },
                "Kafka producer: {Message}", m.Message))
        .Build();

    /// <summary>
    /// Publishes the specified integration event asynchronously.
    /// </summary>
    /// <param name="event">The integration event to be published.</param>
    /// <param name="cancellationToken">Optional. The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation. The task result indicates whether the event was successfully published.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="event"/> is null.</exception>
    public async Task<bool> PublishAsync(IntegrationEvent @event, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@event);

        _logger.LogInformation("publishing event {EventId} ...", @event.Id);
        //var eventType = @event.GetType();

        //var serialized = System.Text.Json.JsonSerializer.Serialize(@event, eventType);

        var json = JsonSerializer.Serialize(@event, @event.GetType(), new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });

        var headers = new Headers
            {
                {"id", Encoding.UTF8.GetBytes(@event.Id.ToString())},
                {"sender", Encoding.UTF8.GetBytes(_config.InstanceId)},
                {"type", Encoding.UTF8.GetBytes(@event.GetType().AssemblyQualifiedName!)}
            };

        var message = new Message<Guid, string>()
        {
            Key = @event.Id,
            Value = json,
            Headers = headers
        };

        var result = await _producer.ProduceAsync(_config.TopicName, message, cancellationToken);
        _logger.LogInformation($"Publish event status of {@event.Id}: {result.Message.Value}");
        return result.Status == PersistenceStatus.Persisted;
    }

    /// <summary>
    /// Disposes the resources used by the event producer.
    /// </summary>
    public void Dispose()
    {
        _producer?.Dispose();
    }
}
