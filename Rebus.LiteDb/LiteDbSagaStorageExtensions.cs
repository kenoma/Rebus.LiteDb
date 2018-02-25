using Rebus.Config;
using Rebus.Sagas;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.LiteDb
{
    /// <summary>
    ///     Configuration extensions for LiteDb saga manager
    /// </summary>
    public static class LiteDbSagaStorageExtensions
    {
        /// <summary>
        ///     Configures Rebus to store sagas in LiteDb database.
        /// </summary>
        public static void StoreInLiteDb(this StandardConfigurer<ISagaStorage> configurer, string pathToDatabase)
        {
            if (configurer == null)
                throw new ArgumentNullException(nameof(configurer));

            configurer.Register(c => new LiteDbSagaStorage(pathToDatabase, new CollectionNameCorrector()));
        }
    }

}
