using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Text;

namespace Mehedi.EventBus.Kafka;

/// <summary>
/// Represents a Kafka event producer responsible for publishing integration events.
/// Source: https://github.com/mizrael/SuperSafeBank/blob/master/src/SuperSafeBank.Transport.Kafka/EventProducer.cs
/// </summary>
public class EventProducer : IDisposable, IEventProducer
{
    private IProducer<Guid, string> _producer;
    private readonly KafkaProducerConfig _config;
    private readonly ILogger<EventProducer> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="EventProducer"/> class with the specified Kafka producer configuration and logger.
    /// </summary>
    /// <param name="config">The Kafka producer configuration.</param>
    /// <param name="logger">The logger instance.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="config"/> or <paramref name="logger"/> is null.</exception>
    public EventProducer(KafkaProducerConfig config, ILogger<EventProducer> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _config = config ?? throw new ArgumentNullException(nameof(config));

        var producerConfig = new ProducerConfig { BootstrapServers = config.KafkaConnectionString };
        var producerBuilder = new ProducerBuilder<Guid, string>(producerConfig);
        producerBuilder.SetKeySerializer(new KeySerializer<Guid>());
        _producer = producerBuilder.Build();
    }

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
        var eventType = @event.GetType();

        var serialized = System.Text.Json.JsonSerializer.Serialize(@event, eventType);

        var headers = new Headers
            {
                {"id", Encoding.UTF8.GetBytes(@event.Id.ToString())},
                {"type", Encoding.UTF8.GetBytes(eventType.AssemblyQualifiedName)}
            };

        var message = new Message<Guid, string>()
        {
            Key = @event.Id,
            Value = serialized,
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
        _producer = null;
    }
}
