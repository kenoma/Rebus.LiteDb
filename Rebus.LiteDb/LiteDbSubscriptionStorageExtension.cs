using Rebus.Config;
using Rebus.Subscriptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.LiteDb
{
    public static class LiteDbSubscriptionStorageExtension
    {
        /// <summary>
        ///  Configures Rebus to store subscriptions in LiteDb databse. Database file should be shard between all participants.
        /// </summary>
        public static void StoreInLiteDb(this StandardConfigurer<ISubscriptionStorage> configurer, string pathToDatabase)
        {
            if (configurer == null)
                throw new ArgumentNullException(nameof(configurer));
            if (string.IsNullOrEmpty(pathToDatabase))
                throw new ArgumentNullException(nameof(pathToDatabase));

            configurer.Register(c => new LiteDbSubscriptionStorage(pathToDatabase, new CollectionNameCorrector()));
        }
    }
}
