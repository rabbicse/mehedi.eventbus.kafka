using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Mehedi.EventBus.Kafka;

/// <summary>Registers the Kafka producer and hosted consumers.</summary>
public static class KafkaServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="KafkaEventProducer"/> as the singleton <see cref="IEventProducer"/>.
    /// </summary>
    /// <example>
    /// <code>
    /// services.AddKafkaProducer(new KafkaProducerConfig("localhost:9092", "kyc.customers", "kyc-api"));
    /// </code>
    /// </example>
    public static IServiceCollection AddKafkaProducer(this IServiceCollection services, KafkaProducerConfig config)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(config);

        services.TryAddSingleton(config);
        services.TryAddSingleton<KafkaEventProducer>();
        services.TryAddSingleton<IEventProducer>(sp => sp.GetRequiredService<KafkaEventProducer>());
        return services;
    }

    /// <summary>
    /// Registers a hosted Kafka consumer. Chain <see cref="KafkaConsumerBuilder.AddHandler{TEvent, THandler}"/>
    /// for every event it should handle. Call it more than once for several consumer groups.
    /// </summary>
    /// <example>
    /// <code>
    /// services.AddKafkaConsumer(options =>
    ///     {
    ///         options.BootstrapServers = "localhost:9092";
    ///         options.GroupId = "ordering";
    ///         options.Topics.Add("kyc.customers");
    ///     })
    ///     .AddHandler&lt;CustomerCreatedEvent, CustomerCreatedHandler&gt;();
    /// </code>
    /// </example>
    /// <exception cref="InvalidOperationException">A required option is missing.</exception>
    public static KafkaConsumerBuilder AddKafkaConsumer(this IServiceCollection services, Action<KafkaConsumerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new KafkaConsumerOptions();
        configure(options);
        options.Validate();

        var registry = new IntegrationEventHandlerRegistry();

        // AddHostedService would de-duplicate by type and keep only one consumer, so register the instance directly.
        services.AddSingleton<IHostedService>(sp => new KafkaConsumerBackgroundService(
            options,
            new IntegrationEventDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), registry),
            sp.GetRequiredService<ILogger<KafkaConsumerBackgroundService>>()));

        return new KafkaConsumerBuilder(services, registry, options);
    }
}

/// <summary>Adds event handlers to a consumer registered with <c>AddKafkaConsumer</c>.</summary>
public sealed class KafkaConsumerBuilder
{
    internal KafkaConsumerBuilder(IServiceCollection services, IntegrationEventHandlerRegistry registry, KafkaConsumerOptions options)
    {
        Services = services;
        Registry = registry;
        Options = options;
    }

    /// <summary>The service collection, for further registrations.</summary>
    public IServiceCollection Services { get; }

    /// <summary>The events this consumer routes.</summary>
    public IntegrationEventHandlerRegistry Registry { get; }

    /// <summary>The validated consumer options.</summary>
    public KafkaConsumerOptions Options { get; }

    /// <summary>
    /// Routes <typeparamref name="TEvent"/> (by its <see cref="IntegrationEvent.EventName"/>) to
    /// <typeparamref name="THandler"/>, which is resolved from a new DI scope per message.
    /// </summary>
    public KafkaConsumerBuilder AddHandler<TEvent, THandler>()
        where TEvent : IntegrationEvent
        where THandler : class, IIntegrationEventHandler<TEvent>
    {
        Registry.Register<TEvent>();
        Services.TryAddEnumerable(ServiceDescriptor.Scoped<IIntegrationEventHandler<TEvent>, THandler>());
        return this;
    }
}
