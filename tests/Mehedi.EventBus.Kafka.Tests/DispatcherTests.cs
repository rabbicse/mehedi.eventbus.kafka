using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;

namespace Mehedi.EventBus.Kafka.Tests;

public class DispatcherTests
{
    private static (IntegrationEventDispatcher Dispatcher, RecordingHandler Handler) Create()
    {
        var handler = new RecordingHandler();
        var services = new ServiceCollection();
        services.AddSingleton<IIntegrationEventHandler<CustomerCreated>>(handler);

        var registry = new IntegrationEventHandlerRegistry();
        registry.Register<CustomerCreated>();

        var provider = services.BuildServiceProvider();
        return (new IntegrationEventDispatcher(provider.GetRequiredService<IServiceScopeFactory>(), registry), handler);
    }

    [Fact]
    public async Task A_known_event_is_deserialized_and_handled()
    {
        var (dispatcher, handler) = Create();
        var sent = new CustomerCreated(Guid.NewGuid(), "Ada");

        var outcome = await dispatcher.DispatchAsync(sent.EventName, IntegrationEventSerializer.Serialize(sent), CancellationToken.None);

        Assert.Equal(DispatchOutcome.Handled, outcome);
        var received = Assert.Single(handler.Received);
        Assert.Equal(sent.CustomerId, received.CustomerId);
        Assert.Equal("Ada", received.Name);
        Assert.Equal(sent.Id, received.Id);
    }

    [Fact]
    public async Task An_event_published_with_another_class_but_the_same_name_is_understood()
    {
        var (dispatcher, handler) = Create();
        var sent = new CustomerCreatedCopy(Guid.NewGuid(), "Grace");

        await dispatcher.DispatchAsync(sent.EventName, IntegrationEventSerializer.Serialize(sent), CancellationToken.None);

        Assert.Equal(sent.CustomerId, Assert.Single(handler.Received).CustomerId);
    }

    [Theory]
    [InlineData("unknown.event.v1")]
    [InlineData(null)]
    public async Task An_unregistered_event_is_ignored(string? eventName)
    {
        var (dispatcher, handler) = Create();

        var outcome = await dispatcher.DispatchAsync(eventName, "{}", CancellationToken.None);

        Assert.Equal(DispatchOutcome.Ignored, outcome);
        Assert.Empty(handler.Received);
    }

    [Fact]
    public async Task A_malformed_payload_is_a_json_exception()
    {
        var (dispatcher, _) = Create();

        await Assert.ThrowsAnyAsync<JsonException>(() =>
            dispatcher.DispatchAsync("test.customer-created.v1", "not json", CancellationToken.None));
    }

    [Fact]
    public async Task Handler_exceptions_reach_the_caller_so_it_can_retry()
    {
        var services = new ServiceCollection();
        services.AddScoped<IIntegrationEventHandler<CustomerCreated>, FailingHandler>();
        var registry = new IntegrationEventHandlerRegistry();
        registry.Register<CustomerCreated>();
        var dispatcher = new IntegrationEventDispatcher(
            services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(), registry);
        var sent = new CustomerCreated(Guid.NewGuid(), "Ada");

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            dispatcher.DispatchAsync(sent.EventName, IntegrationEventSerializer.Serialize(sent), CancellationToken.None));
    }
}
