using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Mehedi.EventBus.Kafka.Tests;

public class RegistrationTests
{
    [Fact]
    public void Two_types_cannot_share_a_name_in_one_consumer()
    {
        var registry = new IntegrationEventHandlerRegistry();
        registry.Register<CustomerCreated>();

        Assert.Throws<InvalidOperationException>(() => registry.Register<CustomerCreatedCopy>());
    }

    [Fact]
    public void Registering_the_same_type_twice_is_harmless()
    {
        var registry = new IntegrationEventHandlerRegistry();
        registry.Register<CustomerCreated>();
        registry.Register<CustomerCreated>();

        Assert.Single(registry.EventNames);
    }

    [Fact]
    public void Each_consumer_is_its_own_hosted_service()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddKafkaConsumer(o => { o.BootstrapServers = "localhost:9092"; o.GroupId = "a"; o.Topics.Add("t1"); })
            .AddHandler<CustomerCreated, RecordingHandler>();
        services.AddKafkaConsumer(o => { o.BootstrapServers = "localhost:9092"; o.GroupId = "b"; o.Topics.Add("t2"); })
            .AddHandler<CustomerCreated, RecordingHandler>();

        using var provider = services.BuildServiceProvider();
        Assert.Equal(2, provider.GetServices<IHostedService>().Count());
        Assert.Single(provider.CreateScope().ServiceProvider.GetServices<IIntegrationEventHandler<CustomerCreated>>());
    }

    [Theory]
    [InlineData("", "group", "topic")]
    [InlineData("localhost:9092", "", "topic")]
    [InlineData("localhost:9092", "group", null)]
    public void Missing_required_options_fail_at_registration(string servers, string group, string? topic)
    {
        var services = new ServiceCollection();

        Assert.Throws<InvalidOperationException>(() => services.AddKafkaConsumer(o =>
        {
            o.BootstrapServers = servers;
            o.GroupId = group;
            if (topic is not null)
            {
                o.Topics.Add(topic);
            }
        }));
    }

    [Fact]
    public void The_producer_is_registered_as_the_event_producer()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKafkaProducer(new KafkaProducerConfig("localhost:9092", "topic", "test"));

        Assert.Contains(services, d => d.ServiceType == typeof(IEventProducer));
    }
}
