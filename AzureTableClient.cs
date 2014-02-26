using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace AzureStorageRest
{
    public class AzureTableClient : AzureStorageClient
    {
        public AzureTableClient(string storageAccount, HttpMessageHandler handler)
            : base(storageAccount, handler)
        {
        }

        public AzureTableClient(string storageAccount, string sharedAccessSignature)
            : base(storageAccount, sharedAccessSignature)
        {
        }

        private async Task<HttpResponseMessage> GetResponse(string tableName, IEnumerable<KeyValuePair<string, string>> parameters, Tuple<string, string> continuationToken = null)
        {
            var queryParts = new List<string> {SAS};
            if (continuationToken != null)
            {
                queryParts.Add("NextPartitionKey=" + continuationToken.Item1);
                queryParts.Add("NextRowKey=" + continuationToken.Item2);
            }
            queryParts.AddRange(FormatParameters(parameters));
            var query = QueryPartsToString(queryParts);
            var uri = string.Format(@"http://{0}.table.core.windows.net/{1}{2}", StorageAccount, tableName, query);

            var msg = new HttpRequestMessage(HttpMethod.Get, uri);
//            msg.Headers.Add("x-ms-client-request-id", Guid.NewGuid().ToString());
//            msg.Headers.Add("x-ms-date", DateTime.UtcNow.ToString("r"));
            var resp = await Cli.SendAsync(msg);
            return resp;
        }

        private IObservable<HttpResponseMessage> GetResponses(string tableName, IDictionary<string, string> parameters)
        {
            var o = Observable.Create<HttpResponseMessage>(observer =>
            {
                var subscription = new BooleanDisposable();
                Tuple<string, string> continuationToken = null;
                Action work = async () =>
                {
                    while (true)
                    {
                        HttpResponseMessage resp;
                        try
                        {
                            if (subscription.IsDisposed)
                                return;
                            resp = await GetResponse(tableName, parameters, continuationToken);

                            IEnumerable<string> rk;
                            continuationToken = resp.Headers.TryGetValues("x-ms-continuation-NextRowKey", out rk)
                                    ? Tuple.Create(resp.Headers.GetValues("x-ms-continuation-NextPartitionKey").First(), rk.First())
                                    : null;

                            if (subscription.IsDisposed)
                                return;
                        }
                        catch (Exception e)
                        {
                            observer.OnError(e);
                            return;
                        }
                        observer.OnNext(resp);
                        if (continuationToken == null)
                            break;
                    }
                    observer.OnCompleted();
                };
                work();
                return subscription;
            });
            return o;
        }

        private static IEnumerable<string> FormatParameters(IEnumerable<KeyValuePair<string, string>> parameters)
        {
            return parameters == null 
                ? Enumerable.Empty<string>() 
                : parameters.Select(x => '$' + x.Key + '=' + x.Value);
        }

        private static string QueryPartsToString(IEnumerable<string> parts)
        {
            var query = parts
                .Where(x => !string.IsNullOrEmpty(x))
                .Aggregate("", (s, p) => s + p + '&')
                .TrimEnd('&');
            if (query[0] != '?')
                query = '?' + query;
            return query;
        }

        public async Task<string> GetString(string tableName, IDictionary<string, string> parameters = null)
        {
            var resp = await GetResponse(tableName, parameters);
            return await resp.Content.ReadAsStringAsync();
        }

        private async Task<Stream> GetStream(string tableName, IEnumerable<KeyValuePair<string, string>> parameters = null)
        {
            var resp = await GetResponse(tableName, parameters);
            return await resp.Content.ReadAsStreamAsync();
        }

        public IObservable<IEnumerable<TableEntry>> GetEntriesPaged(string tableName, IDictionary<string, string> parameters = null)
        {
            return
                GetResponses(tableName, parameters)
                    .SelectMany(x => x.Content.ReadAsStreamAsync())
                    .Select(EntriesFromStream);
        }

        public async Task<IEnumerable<TableEntry>> GetEntries(string tableName, IDictionary<string, string> parameters = null)
        {
            var stream = await GetStream(tableName, parameters);
            return EntriesFromStream(stream);
        }

        private static IEnumerable<TableEntry> EntriesFromStream(Stream stream)
        {
            var xml = XElement.Load(stream);
            XNamespace mNS = "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata";
            var entries = xml.Descendants(mNS + "properties").Select(EntryFromProperties);
            return entries;
        }

        private static TableEntry EntryFromProperties(XElement el)
        {
            XNamespace dataNs = "http://schemas.microsoft.com/ado/2007/08/dataservices";
            XNamespace metaNs = "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata";
            string rowKey = null;
            var timestamp = DateTime.MinValue;
            var properties = new Dictionary<string, object>();
            foreach (var property in el.Elements())
            {
                if (property.Name == dataNs + "RowKey")
                    rowKey = property.Value;
                else if (property.Name == dataNs + "PartitionKey") { }
                else if (property.Name == dataNs + "Timestamp")
                    timestamp = DateTime.Parse(property.Value);
                else
                {
                    var propTypeAttr = property.Attribute(metaNs + "type");
                    if (propTypeAttr != null && propTypeAttr.Value == "Edm.DateTime")
                        properties[property.Name.LocalName] = DateTime.Parse(property.Value);
                    else
                        properties[property.Name.LocalName] = property.Value;
                }
            }
            return new TableEntry(rowKey, properties)
                       {
                           Timestamp = timestamp,
                       };
        }

        private static XDocument CreateEntryXML(string partitionKey, string rowKey, IEnumerable<KeyValuePair<string, object>> data)
        {
            XNamespace defNs = "http://www.w3.org/2005/Atom";
            XNamespace metaNs = "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata";
            XNamespace dataNs = "http://schemas.microsoft.com/ado/2007/08/dataservices";
            var propsXml = new XElement(metaNs + "properties", data.Select(x =>
            {
                if (x.Value is DateTime)
                    return new XElement(dataNs + x.Key, new XAttribute(metaNs + "type", "Edm.DateTime"), x.Value);
                return new XElement(dataNs + x.Key, x.Value);
            }));
            propsXml.Add(new XElement(dataNs + "PartitionKey", partitionKey));
            propsXml.Add(new XElement(dataNs + "RowKey", rowKey));
            var xml = new XElement(defNs + "entry",
                new XElement(defNs + "content", 
                    new XAttribute("type", "application/xml"), 
                    propsXml));
            xml.SetAttributeValue(XNamespace.Xmlns + "m", metaNs);
            xml.SetAttributeValue(XNamespace.Xmlns + "d", dataNs);
            var doc = new XDocument(xml);
            return doc;
        }

        public async Task<string> Post(string table, string partitionKey, TableEntry entry)
        {
            var ms = new MemoryStream();
            var doc = CreateEntryXML(partitionKey, entry.RowKey, entry.Properties);
            doc.Save(ms, SaveOptions.DisableFormatting);
            ms.Seek(0, SeekOrigin.Begin);
            var content = new StreamContent(ms);
            content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/atom+xml");
            var uri = string.Format(@"http://{0}.table.core.windows.net/{1}{2}", StorageAccount, table, SAS);
            var resp = await Cli.PostAsync(uri, content);
            return await resp.Content.ReadAsStringAsync();
        }

        public async Task<string> PostMultipart(string table, string partitionKey, IEnumerable<TableEntry> entries)
        {
            var batchId = Guid.NewGuid().ToString();
            var changeSetId = Guid.NewGuid().ToString();
            var tableUri = string.Format(@"http://{0}.table.core.windows.net/{1}", StorageAccount, table);
            var entryPrefix = "\n--changeset_" + changeSetId +
                              "\nContent-Type: application/http" +
                              "\nContent-Transfer-Encoding: binary" +
                              "\n\nPOST " + tableUri + " HTTP/1.1" +
                              "\nContent-Type: application/atom+xml;type=entry;charset=utf-8\n";

            var stream = new MemoryStream();
            var writer = new StreamWriter(stream);
            writer.WriteLine("--batch_" + batchId +
                       "\nContent-Type: multipart/mixed; boundary=changeset_" + changeSetId);
            foreach (var doc in entries.Select(x => CreateEntryXML(partitionKey, x.RowKey, x.Properties)))
            {
                writer.WriteLine(entryPrefix);
                writer.Flush();
                doc.Save(stream, SaveOptions.DisableFormatting);
            }
            writer.WriteLine("\n--changeset_" + changeSetId + "--" +
                       "\n--batch_" + batchId + "--");
            writer.Flush();
            stream.Seek(0, SeekOrigin.Begin);
            var content = new StreamContent(stream);
            content.Headers.Add("x-ms-version", "2009-04-14");
            content.Headers.ContentType = MediaTypeHeaderValue.Parse("multipart/mixed; boundary=batch_" + batchId);

            var uri = string.Format(@"http://{0}.table.core.windows.net/$batch{1}", StorageAccount, SAS);
            var resp = await Cli.PostAsync(uri, content);
            return await resp.Content.ReadAsStringAsync();
        }

        public static string FormatFilter(string format, params object[] args)
        {
            for (var i = 0; i < args.Length; i++)
            {
                if (args[i] is DateTime)
                    args[i] = "datetime'" + Uri.EscapeDataString(((DateTime)args[i]).ToString("o")) + "'";
                else
                    args[i] = "'" + Uri.EscapeDataString(args[i].ToString()) + "'";
            }
            return string.Format(format, args);
        }
    }

    public class TableEntry
    {
        public TableEntry(string rowKey, IDictionary<string, object> properties)
        {
            RowKey = rowKey;
            Properties = properties;
        }

        public TableEntry(IDictionary<string, object> properties)
        {
            RowKey = Guid.NewGuid().ToString();
            Properties = properties;
        }

        public string RowKey { get; private set; }
        public IDictionary<string, object> Properties { get; set; }
        public DateTime Timestamp { get; set; }
    }
}
