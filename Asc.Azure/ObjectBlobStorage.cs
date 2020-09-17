using Asc.Azure.Abstractions;
using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Asc.Azure
{
    public class ObjectBlobStorage<T> : IObjectBlobStorage<T>
    {
        private readonly BlobStorage blobStorage;

        public ObjectBlobStorage(string connectionString, string containerName)
        {
            blobStorage = new BlobStorage(connectionString, containerName);
        }

        public async Task<T> Get(Uri uri)
        {
            var bytes = await blobStorage.Get(uri);
            var json = Encoding.UTF8.GetString(bytes);
            return JsonSerializer.Deserialize<T>(json);
        }

        public async Task<Uri> Save(string filename, T obj)
        {
            var json = JsonSerializer.Serialize(obj);
            var bytes = Encoding.UTF8.GetBytes(json);
            return await blobStorage.Save(filename, bytes);
        }
    }
}
