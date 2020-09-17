using Asc.Azure.Abstractions;
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

        public TableRepository(CloudTable table)
        {
            this.table = table;
        }

        public TableRepository(string connectionString, string tableName)
        {
            var storageAcc = CloudStorageAccount.Parse(connectionString);
            var tableClient = storageAcc.CreateCloudTableClient(new TableClientConfiguration());
            table = tableClient.GetTableReference(tableName);
        }

        public async Task<T> Get(string partitionKey, string rowKey)
        {
            TableOperation readOperation = TableOperation.Retrieve<T>(partitionKey, rowKey);
            TableResult result = await table.ExecuteAsync(readOperation);
            return result.Result as T;
        }

        private async Task<IList<T>> ExecuteQueryAsync(
            TableQuery<T> query,
            CancellationToken ct = default,
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

        public async Task<IList<T>> GetByPartition(string partitionKey)
        {
            var query = new TableQuery<T>().Where(
              TableQuery.GenerateFilterCondition(
                "PartitionKey",
                QueryComparisons.GreaterThanOrEqual,
                partitionKey
              )
            );

            var items = await ExecuteQueryAsync(query);

            return items;
        }

        public async Task Remove(T entity)
        {
            TableOperation deleteOperation = TableOperation.Delete(entity);
            await table.ExecuteAsync(deleteOperation);
        }

        public async Task<T> Save(T entity)
        {
            TableOperation insertOperation = TableOperation.InsertOrMerge(entity);
            TableResult result = await table.ExecuteAsync(insertOperation);
            return result.Result as T;
        }
    }
}
