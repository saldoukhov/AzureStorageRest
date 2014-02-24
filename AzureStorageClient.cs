using System.Net.Http;

namespace AzureStorageRest
{
    public class AzureStorageClient
    {
        protected string StorageAccount;
        protected string SAS;
        protected HttpClient Cli;

        public AzureStorageClient(string storageAccount, HttpMessageHandler handler)
        {
            StorageAccount = storageAccount;
            Cli = new HttpClient(handler);
        }

        public AzureStorageClient(string storageAccount, string sharedAccessSignature)
        {
            StorageAccount = storageAccount;
            SAS = sharedAccessSignature;
            Cli = new HttpClient();
            Cli.DefaultRequestHeaders.Add("MaxDataServiceVersion", "3.0;NetFx");
        }
    }
}
