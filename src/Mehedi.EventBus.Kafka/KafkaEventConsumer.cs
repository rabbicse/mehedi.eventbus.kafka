using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Runtime.Serialization;
using System.Text;

namespace Mehedi.EventBus.Kafka;

/// <summary>
/// Represents a Kafka event consumer responsible for consuming integration events.
/// For new code prefer <c>services.AddKafkaConsumer(...).AddHandler&lt;TEvent, THandler&gt;()</c>, which adds
/// at-least-once delivery, retries and a dead-letter topic, and does not need the producer's .NET types.
/// Source: https://github.com/mizrael/SuperSafeBank/blob/master/src/SuperSafeBank.Transport.Kafka/KafkaEventConsumer.cs
/// </summary>
public class KafkaEventConsumer(
    KafkaConsumerConfig config,
    ILogger<KafkaEventConsumer> logger) : IDisposable, IEventConsumer
{
    private readonly KafkaConsumerConfig _config = config;
    private readonly ILogger<KafkaEventConsumer> _logger = logger;
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
    .SetErrorHandler((_, e) => logger.LogError($"Kafka Consumer: Error => {e.Reason}"))
    .SetLogHandler((_, m) => logger.Log(
        m.Level switch { SyslogLevel.Error => LogLevel.Error, _ => LogLevel.Information },
        $"Kafka Consumer: {m.Message}"))
    .Build();

    /// <summary>
    /// Subscribe to topic
    /// </summary>
    /// <param name="topicName"></param>
    /// <returns></returns>
    public async Task<bool> Subscribe(string topicName)
    {
        _consumer.Subscribe(topicName);
        return await Task.FromResult(true);
    }

    /// <summary>
    /// Start consuming integration events asynchronously.
    /// </summary>
    /// <param name="cancellationToken">Optional. The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public Task StartConsumeAsync(CancellationToken cancellationToken = default)
    {
        return Task.Run(async () =>
        {            
            var topics = string.Join(",", _consumer.Subscription);
            _logger.LogInformation($"Kafka Consumer: Started Kafka consumer {_consumer.Name} on {topics}");

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var cr = _consumer.Consume(cancellationToken);
                    if (cr.IsPartitionEOF)
                    {
                        _logger.LogInformation($"Kafka Consumer: IsPartitionEOF => {cr.IsPartitionEOF}");
                        continue;
                    }

                    var senderIdHeader = cr.Message.Headers.FirstOrDefault(h => h.Key == "sender");
                    var senderId = senderIdHeader != null ? Encoding.UTF8.GetString(senderIdHeader.GetValueBytes()) : "";

                    if (senderId.Equals(_config.InstanceId, StringComparison.OrdinalIgnoreCase))
                    {
                        continue; // Skip self-published message
                    }

                    var messageTypeHeader = cr.Message.Headers.FirstOrDefault(h => h.Key == KafkaHeaders.ClrType);
                    var eventTypeName = messageTypeHeader is null ? null : Encoding.UTF8.GetString(messageTypeHeader.GetValueBytes());
                    var eventType = eventTypeName is null ? null : Type.GetType(eventTypeName);
                    if (eventType is null)
                    {
                        // The producer's contract type is not loaded in this process. Use AddKafkaConsumer with
                        // typed handlers to consume events across services.
                        _logger.LogWarning("Kafka Consumer: skipped a message of unknown type {EventType}", eventTypeName);
                        continue;
                    }

                    var @event = System.Text.Json.JsonSerializer.Deserialize(cr.Message.Value, eventType, IntegrationEventSerializer.Options) as IntegrationEvent;
                    if (null == @event)
                        throw new SerializationException($"unable to deserialize event {eventTypeName}");

                    await OnEventReceived(@event);
                }
                catch (OperationCanceledException ex)
                {
                    _logger.LogWarning(ex, $"Kafka Consumer: Consumer {_consumer.Name} on {topics} was stopped: {ex.Message}");
                    OnConsumerStopped();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Kafka Consumer: An exception has occurred while consuming a message: {ex.Message}");
                    OnExceptionThrown(ex);
                }
            }
        }, cancellationToken);
    }

    /// <summary>
    /// Event raised when an integration event is received.
    /// </summary>
    public event EventReceivedHandler EventReceived;

    protected virtual Task? OnEventReceived(IntegrationEvent @event)
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
    ~KafkaEventConsumer() => Dispose(false);
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
