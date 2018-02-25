using FluentAssertions;
using NUnit.Framework;
using Rebus.Sagas;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Rebus.LiteDb.UnitTests
{
    [TestFixture]
    class LiteDbSagaStorageTests
    {
        string _path;

        [SetUp]
        public void Setup()
        {
            _path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, Path.GetRandomFileName());
        }

        [TearDown]
        public void CleanUp()
        {
            if (File.Exists(_path))
                File.Delete(_path);
        }

        [Test]
        public void Delete()
        {
            var storage = Create();
            var data = new DummySaga<long> { Signature = Environment.TickCount, Id = Guid.NewGuid(), Revision = 0 };

            storage.Insert(data, new[] { new DummyCorellation(nameof(DummySaga<long>.Signature), typeof(DummySaga<long>)) }).Wait();
            storage.Delete(data).Wait();

            var stored = storage.Find(typeof(DummySaga<long>), nameof(DummySaga<long>.Signature), data.Signature);
            stored.Wait();

            stored.Result.Should().BeNull();
        }

        [Test]
        public void Insert_CreateNewEntry()
        {
            var storage = Create();
            var data = new DummySaga<long> { Signature = Environment.TickCount, Id = Guid.NewGuid(), Revision = 0 };

            storage.Insert(data, new[] { new DummyCorellation(nameof(DummySaga<long>.Signature), typeof(DummySaga<long>)) }).Wait();

            var stored = storage.Find(typeof(DummySaga<long>), nameof(DummySaga<long>.Signature), data.Signature);
            stored.Wait();

            data.Should().BeEquivalentTo(stored.Result);
        }

        [Test]
        async public Task Insert_CreateMixedGenericsItems()
        {
            var storage = Create();
            var data1 = new DummySaga<string> { Signature = Path.GetRandomFileName(), Id = Guid.NewGuid(), Revision = 0 };
            var data2 = new DummySaga<long> { Signature = Environment.TickCount, Id = Guid.NewGuid(), Revision = 0 };

            storage.Insert(data1, new[] { new DummyCorellation(nameof(DummySaga<string>.Signature), typeof(DummySaga<string>)) }).Wait();
            storage.Insert(data2, new[] { new DummyCorellation(nameof(DummySaga<long>.Signature), typeof(DummySaga<long>)) }).Wait();

            var stored1 = await storage.Find(typeof(DummySaga<string>), nameof(DummySaga<string>.Signature), data1.Signature);
            var stored2 = await storage.Find(typeof(DummySaga<long>), nameof(DummySaga<long>.Signature), data2.Signature);

            data1.Should().BeEquivalentTo(stored1);
            data2.Should().BeEquivalentTo(stored2);
        }

        [Test]
        public void Insert_ThrowsIfSameCorellationPropertyInserted()
        {
            var storage = Create();
            var data = new DummySaga<long> { Signature = Environment.TickCount, Id = Guid.NewGuid(), Revision = 0 };
            var data2 = new DummySaga<long> { Signature = data.Signature, Id = Guid.NewGuid(), Revision = 0 };

            storage.Insert(data, new[] { new DummyCorellation(nameof(DummySaga<long>.Signature), typeof(DummySaga<long>)) }).Wait();
            Assert.Throws<AggregateException>(() => storage.Insert(data2, new[] { new DummyCorellation(nameof(DummySaga<long>.Signature), typeof(DummySaga<long>)) }).Wait());
        }

        [Test]
        public void Update_NormalCase()
        {
            var storage = Create();
            var data = new DummySaga<long> { Signature = Environment.TickCount, Id = Guid.NewGuid(), Revision = 0 };
            storage.Insert(data, new[] { new DummyCorellation(nameof(DummySaga<long>.Signature), typeof(DummySaga<long>)) }).Wait();

            data.AnValue = Environment.TickCount;
            storage.Update(data, new[] { new DummyCorellation(nameof(DummySaga<long>.Signature), typeof(DummySaga<long>)) }).Wait();

            var stored = storage.Find(typeof(DummySaga<long>), nameof(DummySaga<long>.Signature), data.Signature);
            stored.Wait();

            data.Should().BeEquivalentTo(stored.Result);
        }

        public LiteDbSagaStorage Create()
        {
            return new LiteDbSagaStorage(_path,new CollectionNameCorrector());
        }

        private class DummySaga<T> : ISagaData
        {
            public Guid Id { get; set; }
            public int Revision { get; set; }

            public T Signature { get; set; }
            public int AnValue { get; set; }
        }

        private class DummyCorellation : ISagaCorrelationProperty
        {
            private string _propertyName;
            private Type _sagaDataType;

            public DummyCorellation(string propertyName, Type sagaDataType)
            {
                _propertyName = propertyName;
                _sagaDataType = sagaDataType;
            }

            public string PropertyName => _propertyName;

            public Type SagaDataType => _sagaDataType;
        }
    }
}
