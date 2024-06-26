using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace JhateSon;

public record Event(string Es, string Em, JsonDocument Ed);

public class Class1
{
    public void wat(ReadOnlyMemory<byte> input, Type expectedType, JsonSerializerOptions jsOptions = null, JsonDocumentOptions jdOptions = default)
    {
        var jd = JsonDocument.Parse(input, jdOptions);
        jd.Deserialize(expectedType, jsOptions);
        JsonTypeInfo.CreateJsonTypeInfo(expectedType, jsOptions).
        JsonSerializer.ser
    }
}