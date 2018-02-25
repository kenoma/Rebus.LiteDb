using FluentAssertions;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.LiteDb.UnitTests
{
    [TestFixture]
    class LiteDbSubscriptionStorageTests
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
        async public Task RegisterSubscriber_SubscriberIsInCollection()
        {
            var storage = Create();
            var topic = Path.GetRandomFileName();
            var address = Path.GetRandomFileName();

            await storage.RegisterSubscriber(topic, address);

            var stored = await storage.GetSubscriberAddresses(topic);

            stored.Contains(address);
        }

        [Test]
        async public Task UnregisterSubscriber_CollectionIsEmpty()
        {
            var storage = Create();
            var topic = Path.GetRandomFileName();
            var address = Path.GetRandomFileName();

            await storage.RegisterSubscriber(topic, address);
            await storage.UnregisterSubscriber(topic, address);

            var stored = await storage.GetSubscriberAddresses(topic);

            stored.Should().BeEmpty();
        }

        public LiteDbSubscriptionStorage Create()
        {
            return new LiteDbSubscriptionStorage(_path, new CollectionNameCorrector());
        }

    }
}
