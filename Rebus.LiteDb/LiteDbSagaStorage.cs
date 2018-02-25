using LiteDB;
using Rebus.Exceptions;
using Rebus.Sagas;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

[assembly:InternalsVisibleTo("Rebus.LiteDb.UnitTests")]
namespace Rebus.LiteDb
{
    internal class LiteDbSagaStorage : ISagaStorage
    {
        private readonly string _pathToStorage;
        private readonly CollectionNameCorrector _collectionNameCorrector;

        public LiteDbSagaStorage(string pathToStorage, CollectionNameCorrector collectionNameCorrector)
        {
            _pathToStorage = pathToStorage;
            _collectionNameCorrector = collectionNameCorrector;
        }

        async public Task Delete(ISagaData sagaData)
        {
            var id = GetId(sagaData);

            using (var db = new LiteDatabase(_pathToStorage))
            {
                var collectionName = _collectionNameCorrector.CorrectName(sagaData.GetType().Name);
                var stored = db.GetCollection<ISagaData>(collectionName);
                stored.Delete(z => z.Id == id);
                sagaData.Revision++;
            }
        }

        async public Task<ISagaData> Find(Type sagaDataType, string propertyName, object propertyValue)
        {
            var valueFromMessage = new BsonValue(propertyValue);

            using (var db = new LiteDatabase(_pathToStorage))
            {
                var collectionName = _collectionNameCorrector.CorrectName(sagaDataType.Name);
                var stored = db.GetCollection<ISagaData>(collectionName);
                var search = stored.Find(Query.EQ(propertyName, valueFromMessage));
                return search.SingleOrDefault();
            }
        }

        /// <summary>
        /// Saves the given saga data, throwing an exception if the instance already exists
        /// </summary>
        async public Task Insert(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            var id = GetId(sagaData);

            if (sagaData.Revision != 0)
            {
                throw new InvalidOperationException($"Attempted to insert saga data with ID {id} and revision {sagaData.Revision}, but revision must be 0 on first insert!");
            }

            using (var db = new LiteDatabase(_pathToStorage))
            {
                var collectionName = _collectionNameCorrector.CorrectName(sagaData.GetType().Name);
                var stored = db.GetCollection<ISagaData>(collectionName);

                foreach (var property in correlationProperties)
                {
                    var valueFromSagaData = GetBsonValue(sagaData, property.PropertyName);
                    var search = stored.Find(Query.EQ(property.PropertyName, valueFromSagaData));
                    if (search.Any())
                    {
                        throw new Exception($"Collision on correlation property {property.PropertyName} : {valueFromSagaData}");
                    }
                }
                stored.Insert(sagaData);
            }
        }

        async public Task Update(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            var id = GetId(sagaData);

            using (var db = new LiteDatabase(_pathToStorage))
            {
                var collectionName = _collectionNameCorrector.CorrectName(sagaData.GetType().Name);
                var stored = db.GetCollection<ISagaData>(collectionName);

                var existingCopy = stored.FindById(id);
                if (existingCopy == null)
                {
                    throw new ConcurrencyException($"Saga data with ID {id} no longer exists and cannot be updated");
                }

                foreach (var property in correlationProperties)
                {
                    var valueFromSagaData = GetBsonValue(sagaData, property.PropertyName);
                    var search = stored.Find(Query.EQ(property.PropertyName, valueFromSagaData));
                    if (search.Any(z => z.Id != id))
                    {
                        throw new Exception($"Attempt to update another existing saga with id {id}");
                    }
                }

                if (existingCopy.Revision != sagaData.Revision)
                {
                    throw new ConcurrencyException($"Attempted to update saga data with ID {id} with revision {sagaData.Revision}, but the existing data was updated to revision {existingCopy.Revision}");
                }

                sagaData.Revision++;
                stored.Update(sagaData);
            }
        }

        static Guid GetId(ISagaData sagaData)
        {
            var id = sagaData.Id;

            if (id != Guid.Empty)
                return id;

            throw new InvalidOperationException("Saga data must be provided with an ID in order to do this!");
        }

        private static BsonValue GetBsonValue(object obj, string path)
        {
            var dots = path.Split('.');

            foreach (var dot in dots)
            {
                var propertyInfo = obj.GetType().GetProperty(dot);
                if (propertyInfo == null)
                    return null;

                obj = propertyInfo.GetValue(obj, new object[0]);
                if (obj == null)
                    break;
            }

            return new BsonValue(obj);
        }
    }
}
