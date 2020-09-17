using Asc.Azure.Abstractions;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Asc.Azure
{
    public class BlobStorage : IBlobStorage
    {
        private readonly CloudBlobContainer container;

        public BlobStorage(string connectionString, string containerName)
        {
            var storageAcc = CloudStorageAccount.Parse(connectionString);
            var cloudBlobClient = storageAcc.CreateCloudBlobClient();
            container = cloudBlobClient.GetContainerReference(containerName);
        }

        public async Task<byte[]> Get(Uri uri)
        {
            var blockBlob = await container.ServiceClient.GetBlobReferenceFromServerAsync(uri);
            using (var ms = new MemoryStream())
            {
                blockBlob.DownloadToStream(ms);
                var jsonBytes = ms.ToArray();
                return jsonBytes;
            }
        }

        public async Task<Uri> Save(string filename, byte[] bytes)
        {
            var blockBlob = container.GetBlockBlobReference(filename);
            using (var ms = new MemoryStream(bytes))
            {
                await blockBlob.UploadFromStreamAsync(ms);
            }
            return blockBlob.Uri;
        }
    }
}
