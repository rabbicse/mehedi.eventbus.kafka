using Confluent.Kafka;
using System.Text;
using System.Text.Json;

namespace Mehedi.EventBus.Kafka;


/// <summary>
/// Represents a serializer for keys used in Kafka messages.
/// </summary>
internal class KeySerializer<TKey> : ISerializer<TKey>
{
    /// <summary>
    /// Serializes the specified key data.
    /// </summary>
    /// <param name="data">The key data to be serialized.</param>
    /// <param name="context">The serialization context.</param>
    /// <returns>The serialized key data as a byte array.</returns>
    public byte[] Serialize(TKey data, SerializationContext context)
    {
        if (data is Guid g)
            return g.ToByteArray();
        var json = JsonSerializer.Serialize(data);
        return Encoding.UTF8.GetBytes(json);
    }
}
