using Asc.Azure.Abstractions;
using ConsistentSharp;
using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Asc.Azure
{
    public class TableRepository<T> : ITableRepository<T> where T : TableEntity, new()
    {
        private readonly CloudTable table;
        private readonly ConsistentHash ch;

        public TableRepository(CloudTable table, int? totalPartitions = null)
        {
            this.table = table;

            if (totalPartitions.HasValue)
            {
                ch = new ConsistentHash();
                for (var i = 0; i < totalPartitions.Value; i++)
                {
                    ch.Add($"p{i}");
                }
            }
        }

        public TableRepository(string connectionString, string tableName, int? totalPartitions = null)
        {
            var storageAcc = CloudStorageAccount.Parse(connectionString);
            var tableClient = storageAcc.CreateCloudTableClient(new TableClientConfiguration());
            table = tableClient.GetTableReference(tableName);
            table.CreateIfNotExists();

            if (totalPartitions.HasValue)
            {
                ch = new ConsistentHash();
                for (var i = 0; i < totalPartitions.Value; i++)
                {
                    ch.Add($"p{i}");
                }
            }
        }

        public bool IsPartitionKeyHashed()
        {
            return ch != null;
        }

        public async Task<T> GetAsync(string rowKey, string partitionKey = null)
        {
            var pk = partitionKey ?? rowKey;
            pk = IsPartitionKeyHashed() ? ch.Get(rowKey) : pk;

            TableOperation readOperation = TableOperation.Retrieve<T>(pk, rowKey);
            TableResult result = await table.ExecuteAsync(readOperation);
            return result.Result as T;
        }

        public Task<IEnumerable<T>> GetByPartitionAsync(string partitionKey, CancellationToken ct = default,
            Action<IList<T>> onProgress = null)
        {
            var query = new TableQuery<T>().Where(
              TableQuery.GenerateFilterCondition(
                "PartitionKey",
                QueryComparisons.Equal,
                partitionKey
              )
            );

            return GetByQueryAsync(query, ct, onProgress);
        }

        public async Task<IEnumerable<T>> GetByQueryAsync(TableQuery<T> query, CancellationToken ct = default,
            Action<IList<T>> onProgress = null)
        {
            var items = new List<T>();
            TableContinuationToken token = null;
            do
            {
                var seg = await table.ExecuteQuerySegmentedAsync(query, token);
                token = seg.ContinuationToken;
                items.AddRange(seg);
                onProgress?.Invoke(items);
            } while (token != null && !ct.IsCancellationRequested);
            return items;
        }

        public async Task<T> SaveAsync(T entity)
        {
            entity.PartitionKey = entity.PartitionKey ?? entity.RowKey;
            entity.PartitionKey = IsPartitionKeyHashed() ? ch.Get(entity.RowKey) : entity.PartitionKey;

            TableOperation insertOperation = TableOperation.InsertOrMerge(entity);
            TableResult result = await table.ExecuteAsync(insertOperation);
            return result.Result as T;
        }
        public Task DeleteAsync(T entity)
        {
            TableOperation deleteOperation = TableOperation.Delete(entity);
            return table.ExecuteAsync(deleteOperation);
        }
    }
}
