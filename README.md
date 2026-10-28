[![Nuget](https://img.shields.io/nuget/v/Mehedi.EventBus.Kafka)](https://www.nuget.org/packages/Mehedi.EventBus.Kafka/)
[![Nuget](https://img.shields.io/nuget/dt/Mehedi.EventBus.Kafka)](https://www.nuget.org/packages/Mehedi.EventBus.Kafka/)

# Mehedi.EventBus.Kafka
EventBus implementation for Apache Kafka: an idempotent producer, and hosted consumers with typed handlers,
at-least-once delivery, retries and a dead-letter topic.

## Install

```bash
dotnet add package Mehedi.EventBus.Kafka
```

## Publish events

```csharp
builder.Services.AddKafkaProducer(new KafkaProducerConfig(
    kafkaConnectionString: "localhost:9092",
    topicBaseName: "kyc.customers",
    instanceId: "kyc-api"));

// anywhere: inject IEventProducer
await producer.PublishAsync(new CustomerCreatedEvent(customer.Id, customer.FirstName, customer.LastName), cancellationToken);
```

Every message carries these headers:

| Header | Value |
|---|---|
| `event-name` | `IntegrationEvent.EventName`, the stable name consumers route on (see `[IntegrationEventName]`) |
| `type` | assembly-qualified .NET type (for consumers older than 1.1) |
| `id`, `sender`, `content-type` | event id, producer `instanceId`, `application/json` |

The message key is `IntegrationEvent.PartitionKey`. Override it with the aggregate id to keep one aggregate's events in order.
The producer uses `acks=all` and idempotence, so its own retries cause neither duplicates nor reordering.

## Consume events

```csharp
builder.Services
    .AddKafkaConsumer(options =>
    {
        options.BootstrapServers = "localhost:9092";
        options.GroupId = "ordering";            // one group per service
        options.Topics.Add("kyc.customers");
        options.MaxRetryAttempts = 3;            // then dead-letter
        options.DeadLetterTopicSuffix = ".dlq";  // kyc.customers.dlq
    })
    .AddHandler<CustomerCreatedEvent, CustomerCreatedHandler>();

public sealed class CustomerCreatedHandler(OrderingDbContext db) : IIntegrationEventHandler<CustomerCreatedEvent>
{
    public async Task HandleAsync(CustomerCreatedEvent @event, CancellationToken cancellationToken)
    {
        // At-least-once delivery: make it idempotent.
        ...
    }
}
```

What the hosted consumer does for each message:

1. Skips it if `sender` equals `SkipMessagesFromSender` (optional).
2. Looks up the handler(s) by the `event-name` header. For messages from older producers it falls back to the class name in the `type` header. Unknown events are skipped.
3. Deserializes the JSON into **your** contract type, so the producer's assembly is not needed. Then it runs the handlers in a new DI scope.
4. On an exception it retries with a growing delay (`RetryDelay × attempt`), up to `MaxRetryAttempts` times.
5. If all retries fail, or straight away when the payload can't be deserialized, it copies the message to `<topic><DeadLetterTopicSuffix>` with `dlq-*` headers that say why.
6. Only then does it store the offset. A crash before that point re-delivers the message, which is why handlers must be idempotent.

Messages from a partition are handled one at a time, in order. Use more partitions and more instances to scale out.

## Upgrading from 1.0.x

- Nothing breaks: `KafkaEventProducer`, `KafkaEventConsumer` and the config records keep their signatures.
- The producer adds the `event-name` and `content-type` headers. Its key is now `PartitionKey`, which still defaults to the event id.
- librdkafka debug logging is now off by default. Set `KafkaProducerConfig.Debug = "msg,broker"` to turn it back on.
- `KafkaEventConsumer` now reads camelCase payloads correctly, and skips messages whose .NET type it can't load instead of throwing.

## Technologies
- .NET Core 8
- C#
- xUnit

## Prerequisites
- net8.0
- Confluent.Kafka (>= 2.3.0)
- Mehedi.EventBus.Abstractions (>= 1.1.1)
- Microsoft.Extensions.Logging.Abstractions (>= 8.0.1)

## References
- [eShop](https://github.com/dotnet/eShop/tree/main/src/EventBus)
- [SuperSafeBank](https://github.com/mizrael/SuperSafeBank)
