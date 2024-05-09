using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Runtime.Serialization;
using System.Text;

namespace Mehedi.EventBus.Kafka;

/// <summary>
/// Represents a Kafka event consumer responsible for consuming integration events.
/// Source: https://github.com/mizrael/SuperSafeBank/blob/master/src/SuperSafeBank.Transport.Kafka/KafkaEventConsumer.cs
/// </summary>
public class EventConsumer : IDisposable, IEventConsumer
{
    private IConsumer<Guid, string> _consumer;
    private readonly ILogger<EventConsumer> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="EventConsumer"/> class with the specified consumer configuration and logger.
    /// </summary>
    /// <param name="config">The consumer configuration.</param>
    /// <param name="logger">The logger instance.</param>
    public EventConsumer(EventsConsumerConfig config, ILogger<EventConsumer> logger)
    {
        _logger = logger;

        var consumerConfig = new ConsumerConfig
        {
            GroupId = config.ConsumerGroup,
            BootstrapServers = config.KafkaConnectionString,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = true
        };

        var consumerBuilder = new ConsumerBuilder<Guid, string>(consumerConfig);
        var keyDeserializerFactory = new KeyDeserializerFactory();
        consumerBuilder.SetKeyDeserializer(keyDeserializerFactory.Create<Guid>());

        _consumer = consumerBuilder.Build();

        _consumer.Subscribe(config.TopicName);
    }
    ~EventConsumer() => Dispose(false);

    /// <summary>
    /// Starts consuming integration events asynchronously.
    /// </summary>
    /// <param name="cancellationToken">Optional. The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public Task StartConsumeAsync(CancellationToken cancellationToken = default)
    {
        return Task.Run(async () =>
        {
            var topics = string.Join(",", _consumer.Subscription);
            _logger.LogInformation("started Kafka consumer {ConsumerName} on {ConsumerTopic}", _consumer.Name, topics);

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var cr = _consumer.Consume(cancellationToken);
                    if (cr.IsPartitionEOF)
                        continue;

                    var messageTypeHeader = cr.Message.Headers.First(h => h.Key == "type");
                    var eventTypeName = Encoding.UTF8.GetString(messageTypeHeader.GetValueBytes());
                    var eventType = Type.GetType(eventTypeName);
                    var @event = System.Text.Json.JsonSerializer.Deserialize(cr.Message.Value, eventType) as IntegrationEvent;
                    if (null == @event)
                        throw new SerializationException($"unable to deserialize event {eventTypeName} : {cr.Message.Value}");

                    await OnEventReceived(@event);
                }
                catch (OperationCanceledException ex)
                {
                    _logger.LogWarning(ex, "consumer {ConsumerName} on {ConsumerTopic} was stopped: {StopReason}", _consumer.Name, topics, ex.Message);
                    OnConsumerStopped();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"an exception has occurred while consuming a message: {ex.Message}");
                    OnExceptionThrown(ex);
                }
            }
        }, cancellationToken);
    }

    /// <summary>
    /// Event raised when an integration event is received.
    /// </summary>
    public event EventReceivedHandler EventReceived;

    protected virtual Task OnEventReceived(IntegrationEvent @event)
    {
        var handler = EventReceived;
        return handler?.Invoke(this, @event);
    }

    /// <summary>
    /// Event raised when an exception occurs during event consumption.
    /// </summary>
    public event ExceptionThrownHandler ExceptionThrown;
    protected virtual void OnExceptionThrown(Exception e)
    {
        var handler = ExceptionThrown;
        handler?.Invoke(this, e);
    }

    public delegate void ConsumerStoppedHandler(object sender);
    /// <summary>
    /// Event raised when the consumer is stopped.
    /// </summary>
    public event ConsumerStoppedHandler ConsumerStopped;

    protected virtual void OnConsumerStopped()
    {
        var handler = ConsumerStopped;
        handler?.Invoke(this);
    }

    #region IDisposable
    private bool _disposed = false;
    protected virtual void Dispose(bool disposing)
    {
        if (_disposed) return;

        if (disposing)
        {
            // Free unmanaged resources
            _consumer?.Dispose();
            _consumer = null;
        }

        _disposed = true;
    }
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
    #endregion
}
