using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Runtime.Serialization;
using System.Text;

namespace Mehedi.EventBus.Kafka;

/// <summary>
/// Represents a Kafka event consumer responsible for consuming integration events.
/// Source: https://github.com/mizrael/SuperSafeBank/blob/master/src/SuperSafeBank.Transport.Kafka/KafkaEventConsumer.cs
/// </summary>
public class EventConsumer(
    EventsConsumerConfig config,
    ILogger<EventConsumer> logger) : IDisposable, IEventConsumer
{
    private readonly EventsConsumerConfig _config = config;
    private readonly ILogger<EventConsumer> _logger = logger;
    private readonly IConsumer<Guid, string> _consumer = new ConsumerBuilder<Guid, string>(new ConsumerConfig
    {
        GroupId = config.ConsumerGroup,
        BootstrapServers = config.KafkaConnectionString,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnablePartitionEof = true,
        EnableAutoCommit = true,
        AllowAutoCreateTopics = true
    })
    .SetKeyDeserializer(new KeyDeserializerFactory().Create<Guid>())
    .SetErrorHandler((_, e) => logger.LogError($"Kafka consumer error: {e.Reason}"))
    .SetLogHandler((_, m) => logger.Log(
        m.Level switch { SyslogLevel.Error => LogLevel.Error, _ => LogLevel.Information },
        $"Kafka log: {m.Message}"))
    .Build();

    /// <summary>
    /// Starts consuming integration events asynchronously.
    /// </summary>
    /// <param name="cancellationToken">Optional. The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public Task StartConsumeAsync(CancellationToken cancellationToken = default)
    {
        return Task.Run(async () =>
        {
            _consumer.Subscribe(_config.TopicName);

            var topics = string.Join(",", _consumer.Subscription);
            _logger.LogInformation("started Kafka consumer {ConsumerName} on {ConsumerTopic}", _consumer.Name, topics);

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var cr = _consumer.Consume(cancellationToken);
                    if (cr.IsPartitionEOF)
                    {
                        _logger.LogInformation($"IsPartitionEOF => {cr.IsPartitionEOF}");
                        continue;
                    }

                    var senderIdHeader = cr.Message.Headers.FirstOrDefault(h => h.Key == "sender");
                    var senderId = senderIdHeader != null ? Encoding.UTF8.GetString(senderIdHeader.GetValueBytes()) : "";

                    if (senderId.Equals(_config.InstanceId, StringComparison.OrdinalIgnoreCase))
                    {
                        continue; // Skip self-published message
                    }

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
    private volatile bool _disposed;
    ~EventConsumer() => Dispose(false);
    protected virtual void Dispose(bool disposing)
    {
        if (_disposed) return;

        if (disposing)
        {
            // Free unmanaged resources
            _consumer?.Dispose();
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
