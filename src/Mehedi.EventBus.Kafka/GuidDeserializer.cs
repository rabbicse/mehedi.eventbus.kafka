using Confluent.Kafka;

namespace Mehedi.EventBus.Kafka;

internal class GuidDeserializer : IDeserializer<Guid>
{
    public Guid Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        return new Guid(data);
    }
}
