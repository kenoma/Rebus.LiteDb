using LiteDB;
using Rebus.Subscriptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.LiteDb
{
    class LiteDbSubscriptionStorage : ISubscriptionStorage
    {
        private readonly string _pathToStorage;
        private readonly CollectionNameCorrector _collectionNameCorrector;

        public LiteDbSubscriptionStorage(string path, CollectionNameCorrector collectionNameCorrector)
        {
            if (string.IsNullOrEmpty(path))
                throw new InvalidOperationException($"Cannot instantiate LiteDb database with given path '{path}'");
            IsCentralized = true;
            _pathToStorage = path;
            _collectionNameCorrector = collectionNameCorrector;
        }

        /// <summary>
        ///     Gets all destination addresses for the given topic
        /// </summary>
        public async Task<string[]> GetSubscriberAddresses(string topic)
        {
            if (topic == null)
                throw new ArgumentNullException(nameof(topic));
            topic = _collectionNameCorrector.CorrectName(topic);
            using (var db = new LiteDatabase(_pathToStorage))
            {
                var dbTopics = db.GetCollection<Subscriber>(topic);
                return dbTopics.FindAll().Select(z => z.Address).ToArray();
            }
        }

        /// <summary>
        ///     Registers the given <paramref name="subscriberAddress"/> as a subscriber of the given topic
        /// </summary>
        public async Task RegisterSubscriber(string topic, string subscriberAddress)
        {
            if (topic == null)
                throw new ArgumentNullException(nameof(topic));
            if (subscriberAddress == null)
                throw new ArgumentNullException(nameof(subscriberAddress));
            topic = _collectionNameCorrector.CorrectName(topic);
            using (var db = new LiteDatabase(_pathToStorage))
            {
                var dbTopics = db.GetCollection<Subscriber>(topic);
                dbTopics.Insert(new Subscriber { Address = subscriberAddress });
            }
        }

        /// <summary>
        ///     Unregisters the given <paramref name="subscriberAddress"/> as a subscriber of the given topic
        /// </summary>
        public async Task UnregisterSubscriber(string topic, string subscriberAddress)
        {
            if (topic == null)
                throw new ArgumentNullException(nameof(topic));
            if (subscriberAddress == null)
                throw new ArgumentNullException(nameof(subscriberAddress));
            topic = _collectionNameCorrector.CorrectName(topic);
            using (var db = new LiteDatabase(_pathToStorage))
            {
                var dbTopics = db.GetCollection<Subscriber>(topic);
                if (dbTopics.Delete(Query.EQ(nameof(Subscriber.Address), subscriberAddress)) == 0)
                    throw new Exception($"Cannot delete item '{subscriberAddress}' from '{topic}'");
            }
        }

        /// <summary>
        /// Gets whether the subscription storage is centralized and thus supports bypassing the usual subscription request
        /// (in a fully distributed architecture, a subscription is established by sending a <see cref="SubscribeRequest"/>
        /// to the owner of a given topic, who then remembers the subscriber somehow - if the subscription storage is
        /// centralized, the message exchange can be bypassed, and the subscription can be established directly by
        /// having the subscriber register itself)
        /// </summary>
        public bool IsCentralized { get; }

        private class Subscriber
        {
            public int Id { get; set; }
            public string Address { get; set; }
        }
    }
}
