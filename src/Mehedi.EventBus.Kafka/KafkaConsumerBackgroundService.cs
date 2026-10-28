using System.Globalization;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Mehedi.EventBus.Kafka;

/// <summary>
/// Hosted Kafka consumer with at-least-once delivery.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>An offset is stored only after the message was handled (or dead-lettered), so a crash re-delivers it.</item>
/// <item>A failing handler is retried <see cref="KafkaConsumerOptions.MaxRetryAttempts"/> times with a growing delay.</item>
/// <item>After that, or at once for a payload that cannot be deserialized, the message goes to the dead-letter topic.</item>
/// <item>Messages are handled one at a time, so the order within a partition is preserved.</item>
/// </list>
/// </remarks>
public sealed partial class KafkaConsumerBackgroundService(
    KafkaConsumerOptions options,
    IntegrationEventDispatcher dispatcher,
    ILogger<KafkaConsumerBackgroundService> logger) : BackgroundService
{
    private IProducer<byte[]?, string>? _deadLetterProducer;

    /// <inheritdoc />
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        // Consume() blocks, so run the loop on its own thread instead of a thread-pool thread.
        Task.Factory.StartNew(
                () => ConsumeLoopAsync(stoppingToken),
                stoppingToken,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default)
            .Unwrap();

    /// <inheritdoc />
    public override void Dispose()
    {
        _deadLetterProducer?.Dispose();
        base.Dispose();
    }

    private async Task ConsumeLoopAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.GroupId,
            AutoOffsetReset = options.AutoOffsetReset,
            // Offsets are committed in the background, but only offsets we stored after handling.
            EnableAutoCommit = true,
            EnableAutoOffsetStore = false,
            AllowAutoCreateTopics = true
        };
        options.ConfigureConsumer?.Invoke(config);

        using var consumer = new ConsumerBuilder<byte[]?, string>(config)
            .SetErrorHandler((_, error) => LogKafkaError(logger, error.Code, error.Reason))
            .Build();

        consumer.Subscribe(options.Topics);
        LogStarted(logger, options.GroupId, string.Join(", ", options.Topics));

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<byte[]?, string>? result;
                try
                {
                    result = consumer.Consume(stoppingToken);
                }
                catch (ConsumeException ex)
                {
                    LogConsumeFailed(logger, ex, ex.Error.Reason);
                    continue;
                }

                if (result is null || result.IsPartitionEOF)
                {
                    continue;
                }

                await HandleAsync(result, stoppingToken).ConfigureAwait(false);
                consumer.StoreOffset(result);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal shutdown.
        }
        finally
        {
            // Leave the group cleanly so partitions are reassigned at once, and commit stored offsets.
            consumer.Close();
            LogStopped(logger, options.GroupId);
        }
    }

    private async Task HandleAsync(ConsumeResult<byte[]?, string> result, CancellationToken stoppingToken)
    {
        var headers = result.Message.Headers;
        var sender = ReadHeader(headers, KafkaHeaders.Sender);

        if (options.SkipMessagesFromSender is { Length: > 0 } self &&
            string.Equals(sender, self, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var eventName = IntegrationEventSerializer.ResolveEventName(
            ReadHeader(headers, KafkaHeaders.EventName),
            ReadHeader(headers, KafkaHeaders.ClrType));

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                var outcome = await dispatcher.DispatchAsync(eventName, result.Message.Value, stoppingToken).ConfigureAwait(false);
                if (outcome == DispatchOutcome.Ignored)
                {
                    LogIgnored(logger, eventName ?? "(none)", result.TopicPartitionOffset.ToString());
                }

                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (JsonException ex)
            {
                // Retrying cannot fix a malformed payload.
                LogPoisonMessage(logger, ex, eventName ?? "(none)", result.TopicPartitionOffset.ToString());
                await DeadLetterAsync(result, ex, stoppingToken).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (attempt <= options.MaxRetryAttempts)
            {
                LogRetrying(logger, ex, eventName ?? "(none)", attempt, options.MaxRetryAttempts);
                await Task.Delay(options.RetryDelay * attempt, stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogGaveUp(logger, ex, eventName ?? "(none)", result.TopicPartitionOffset.ToString());
                await DeadLetterAsync(result, ex, stoppingToken).ConfigureAwait(false);
                return;
            }
        }
    }

    private async Task DeadLetterAsync(
        ConsumeResult<byte[]?, string> result, Exception reason, CancellationToken stoppingToken)
    {
        if (string.IsNullOrEmpty(options.DeadLetterTopicSuffix))
        {
            LogDropped(logger, result.TopicPartitionOffset.ToString());
            return;
        }

        var topic = result.Topic + options.DeadLetterTopicSuffix;
        var headers = new Headers();
        foreach (var header in result.Message.Headers)
        {
            headers.Add(header.Key, header.GetValueBytes());
        }

        headers.Add(KafkaHeaders.DeadLetterReason, Encoding.UTF8.GetBytes($"{reason.GetType().Name}: {reason.Message}"));
        headers.Add(KafkaHeaders.DeadLetterOriginalTopic, Encoding.UTF8.GetBytes(result.Topic));
        headers.Add(KafkaHeaders.DeadLetterOriginalPartition, Encoding.UTF8.GetBytes(result.Partition.Value.ToString(CultureInfo.InvariantCulture)));
        headers.Add(KafkaHeaders.DeadLetterOriginalOffset, Encoding.UTF8.GetBytes(result.Offset.Value.ToString(CultureInfo.InvariantCulture)));

        var message = new Message<byte[]?, string> { Key = result.Message.Key, Value = result.Message.Value, Headers = headers };

        // Do not move past the message until it is safely parked: keep trying while the broker is unavailable.
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await GetDeadLetterProducer().ProduceAsync(topic, message, stoppingToken).ConfigureAwait(false);
                LogDeadLettered(logger, result.TopicPartitionOffset.ToString(), topic);
                return;
            }
            catch (ProduceException<byte[]?, string> ex)
            {
                LogDeadLetterFailed(logger, ex, topic, attempt);
                await Task.Delay(TimeSpan.FromSeconds(Math.Min(30, attempt * 2)), stoppingToken).ConfigureAwait(false);
            }
        }
    }

    private IProducer<byte[]?, string> GetDeadLetterProducer()
    {
        if (_deadLetterProducer is not null)
        {
            return _deadLetterProducer;
        }

        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true
        };
        options.ConfigureDeadLetterProducer?.Invoke(config);

        return _deadLetterProducer = new ProducerBuilder<byte[]?, string>(config).Build();
    }

    private static string? ReadHeader(Headers? headers, string key) =>
        headers is not null && headers.TryGetLastBytes(key, out var bytes) ? Encoding.UTF8.GetString(bytes) : null;

    [LoggerMessage(Level = LogLevel.Information, Message = "Kafka consumer group {GroupId} started on {Topics}")]
    private static partial void LogStarted(ILogger logger, string groupId, string topics);

    [LoggerMessage(Level = LogLevel.Information, Message = "Kafka consumer group {GroupId} stopped")]
    private static partial void LogStopped(ILogger logger, string groupId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Kafka error {Code}: {Reason}")]
    private static partial void LogKafkaError(ILogger logger, ErrorCode code, string reason);

    [LoggerMessage(Level = LogLevel.Error, Message = "Consuming from Kafka failed: {Reason}")]
    private static partial void LogConsumeFailed(ILogger logger, Exception exception, string reason);

    [LoggerMessage(Level = LogLevel.Debug, Message = "No handler for event {EventName} at {Position}; skipped")]
    private static partial void LogIgnored(ILogger logger, string eventName, string position);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Handling {EventName} failed (attempt {Attempt} of {MaxRetries} retries); retrying")]
    private static partial void LogRetrying(ILogger logger, Exception exception, string eventName, int attempt, int maxRetries);

    [LoggerMessage(Level = LogLevel.Error, Message = "Handling {EventName} at {Position} failed after all retries")]
    private static partial void LogGaveUp(ILogger logger, Exception exception, string eventName, string position);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {EventName} at {Position} cannot be deserialized")]
    private static partial void LogPoisonMessage(ILogger logger, Exception exception, string eventName, string position);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Message at {Position} moved to dead-letter topic {Topic}")]
    private static partial void LogDeadLettered(ILogger logger, string position, string topic);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message at {Position} dropped: dead-lettering is disabled")]
    private static partial void LogDropped(ILogger logger, string position);

    [LoggerMessage(Level = LogLevel.Error, Message = "Publishing to dead-letter topic {Topic} failed (attempt {Attempt}); retrying")]
    private static partial void LogDeadLetterFailed(ILogger logger, Exception exception, string topic, int attempt);
}
