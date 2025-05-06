using Azure.Storage.Queues;
using Bogus.DataSets;
using ConsoleAppAzureQueueStorage.Utils;
using Serilog;
using Testcontainers.Azurite;

var logger = new LoggerConfiguration()
    .WriteTo.Console()
    .WriteTo.File("testcontainers-azurequeuestorage.tmp")
    .CreateLogger();
logger.Information("***** Iniciando testes com Testcontainers + Azure Queue Storage *****");

CommandLineHelper.Execute("docker container ls",
    "Containers antes da execucao do Testcontainers...");

var azuriteContainer = new AzuriteBuilder()
  .WithImage("mcr.microsoft.com/azure-storage/azurite:3.34.0")
  .Build();
await azuriteContainer.StartAsync();

CommandLineHelper.Execute("docker container ls",
    "Containers apos execucao do Testcontainers...");

var connectionStringQueueStorage = azuriteContainer.GetConnectionString();
const string queue = "queue-teste";
logger.Information($"Connection String = {connectionStringQueueStorage}");
logger.Information($"Queue Endpoint = {azuriteContainer.GetQueueEndpoint()}");
logger.Information($"Queue a ser utilizada nos testes = {queue}");

var queueProducerClient = new QueueClient(connectionStringQueueStorage, queue);
await queueProducerClient.CreateIfNotExistsAsync();
const int maxMessages = 10;
var lorem = new Lorem("pt_BR");
for (int i = 1; i <= maxMessages; i++)
{
    var sentence = lorem.Sentence();
    logger.Information($"Enviando mensagem {i}/{maxMessages}: {sentence}");
    await queueProducerClient.SendMessageAsync(sentence);
}
logger.Information("Pressione ENTER para continuar...");
Console.ReadLine();

var queueConsumerClient = new QueueClient(connectionStringQueueStorage, queue);
var messages = await queueConsumerClient.ReceiveMessagesAsync(maxMessages);
int k = 0;
foreach (var message in messages.Value)
{
    k++;
    logger.Information($"Mensagem recebida {k}/{maxMessages}: {message.MessageText}");
    await queueConsumerClient.DeleteMessageAsync(message.MessageId, message.PopReceipt);
    logger.Information($"Mensagem excluida {k}/{maxMessages}");
    logger.Information("Pressione ENTER para continuar...");
    Console.ReadLine();
}

Console.WriteLine("Testes concluidos com sucesso!");