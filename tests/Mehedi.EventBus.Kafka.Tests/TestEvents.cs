namespace Mehedi.EventBus.Kafka.Tests;

[IntegrationEventName("test.customer-created.v1")]
public sealed record CustomerCreated(Guid CustomerId, string Name) : IntegrationEvent
{
    public override Guid GetPartitionKey() => CustomerId;
}

/// <summary>Same wire name, different class: what a second service would declare.</summary>
[IntegrationEventName("test.customer-created.v1")]
public sealed record CustomerCreatedCopy(Guid CustomerId, string Name) : IntegrationEvent;

public sealed class RecordingHandler : IIntegrationEventHandler<CustomerCreated>
{
    public List<CustomerCreated> Received { get; } = [];

    public Task HandleAsync(CustomerCreated @event, CancellationToken cancellationToken)
    {
        Received.Add(@event);
        return Task.CompletedTask;
    }
}

public sealed class FailingHandler : IIntegrationEventHandler<CustomerCreated>
{
    public Task HandleAsync(CustomerCreated @event, CancellationToken cancellationToken) =>
        throw new InvalidOperationException("boom");
}
