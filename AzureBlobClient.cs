using System.IO;
using System.Net.Http;
using System.Threading.Tasks;

namespace AzureStorageRest
{
    public class AzureBlobClient : AzureStorageClient
    {
        public AzureBlobClient(string storageAccount, HttpMessageHandler handler) : base(storageAccount, handler)
        {
        }

        public AzureBlobClient(string storageAccount, string sharedAccessSignature) : base(storageAccount, sharedAccessSignature)
        {
        }

        public async Task<string> Put(string container, string blob, Stream content)
        {
            var streamContent = new StreamContent(content);
//            content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/atom+xml");
            streamContent.Headers.Add("x-ms-blob-type", "BlockBlob");
            var uri = string.Format(@"http://{0}.blob.core.windows.net/{1}/{2}{3}", StorageAccount, container, blob, SAS);
            var resp = await Cli.PutAsync(uri, streamContent);
            return await resp.Content.ReadAsStringAsync();
        }
    }
}
