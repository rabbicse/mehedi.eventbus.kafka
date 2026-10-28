namespace Mehedi.EventBus.Kafka.Tests;

public class SerializerTests
{
    [Fact]
    public void The_payload_is_camel_case()
    {
        var json = IntegrationEventSerializer.Serialize(new CustomerCreated(Guid.NewGuid(), "Ada"));

        Assert.Contains("\"customerId\"", json, StringComparison.Ordinal);
        Assert.Contains("\"name\"", json, StringComparison.Ordinal);
    }

    [Fact]
    public void The_event_name_header_wins()
    {
        Assert.Equal("a.b.v1", IntegrationEventSerializer.ResolveEventName("a.b.v1", "Some.Type, Asm"));
    }

    [Theory]
    [InlineData("KYC.Application.Events.CustomerCreatedEvent, KYC.Application, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null", "CustomerCreatedEvent")]
    [InlineData("Outer+NestedEvent, Asm", "NestedEvent")]
    [InlineData("PlainEvent", "PlainEvent")]
    public void Legacy_messages_are_named_after_the_class_in_the_type_header(string typeHeader, string expected)
    {
        Assert.Equal(expected, IntegrationEventSerializer.ResolveEventName(null, typeHeader));
    }

    [Fact]
    public void A_message_without_headers_has_no_name()
    {
        Assert.Null(IntegrationEventSerializer.ResolveEventName(null, null));
    }
}
