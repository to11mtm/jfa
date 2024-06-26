using System;
using System.Collections.Generic;

namespace ShotGlass
{
    public interface ISerializedRecord
    {
        string Manifest { get; }
        string Serializer { get; }
        byte[] Payload { get; }
    }

    public interface IHaveManifest
    {
        string Manifest();
    }
    public abstract class RecordSerializer
    {
        public abstract object FromSerializedRecord(ISerializedRecord record);
        public abstract ISerializedRecord ToSerializedRecord<T>(T toSerialize) where T : IHaveManifest;
    }

    public class SimpleRecordSerializerMapper
    {
        private readonly Dictionary<Type, string> _typeToString;
        private readonly Dictionary<string, Type> _stringToType;

        public SimpleRecordSerializerMapper(IEnumerable<(Type, string)> typeToStringMaps,
            IEnumerable<(string, Type)> stringToTypeMapOverrides)
        {
            
        }
    }
    public class JsonRecordSerializer
    {
        
    }
}