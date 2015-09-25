using System;
using EasyNetQ.ProcessManager.State.SqlServer;
using Microsoft.FSharp.Core;
using Nessos.FsPickler.Json;
using FsPickler = Nessos.FsPickler.FsPickler;

namespace Process3
{
    public class Serializer : ISerializer
    {
        private readonly JsonSerializer _p;
        public Serializer()
        {
            _p = new JsonSerializer(omitHeader: new FSharpOption<bool>(true));
        }

        bool ISerializer.CanSerialize<T>()
        {
            return FsPickler.IsSerializableType<T>();
        }

        bool ISerializer.CanSerialize(Type value)
        {
            return FsPickler.IsSerializableType(value);
        }

        T ISerializer.Deserialize<T>(string value)
        {
            return _p.UnPickleOfString<T>(value);
        }

        string ISerializer.Serialize<T>(T value)
        {
            return _p.PickleToString(value);
        }
    }
}
