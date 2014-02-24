AzureStorageRest
================

This is a lightweight portable client (.Net 4.5, Windows Store, Windows Phone 8) for Azure Table storage and Azure Blob storage.

There are two ways to authorize the client:

- Using account key
- Using shared access signature

If account key is used, HttpMessageHandler needs to be passed to provide the message-signing capabilities:

    var cli = new AzureTableClient("myStorage", new AzureTableMessageHandler("myStorage", myKey));

AzureTableMessageHandler handler is a part of another NuGet package, HttpClientExtras. Since cryptography stack is varied from platform to platform, you need to reference appropriate platform adapter (HttpClientExtras.Net, HttpClientExtras.RT, or HttpClientExtras.WP8). You will also need to initialize the platform-specific services with 

    PlatformAdapters.Init();

If SAS (shared access signature) is used, the client initialization is simpler:

    var cli = new AzureTableClient("myStorage", mySAS);

After you created the client, you need a filter:

    var filter = AzureTableClient.FormatFilter(
    "(Timestamp gt {0}) and (PartitionKey eq {1})", DateTime.Now.AddDays(-5), "myPartitionKey");

and you can get your data:

    var entries = await cli.GetEntries("myTable", new Dictionary<string, string> { { "filter", filter }, });

You can also get the paged entries using Reactive Extensions (beware, due to the bug in Azure, currently you cannot use this syntax with SAS):

    cli.GetEntriesPaged("myTable", 
    	new Dictionary<string, string> {{"filter", filter},})
    	.Subscribe(x => 
    	Console.WriteLine(x.Count()));
     