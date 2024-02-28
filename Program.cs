using System.Collections.Concurrent;
using System.Globalization;
using System.Net.Sockets;
using Rinha2024.VirtualDb.IO;

namespace Rinha2024.VirtualDb;

public class Program
{
    private static readonly int WritePipes = int.TryParse(Environment.GetEnvironmentVariable("WRITE_PIPES"), out var writePipes) ? writePipes : 2;
    private static readonly int MainPort = int.TryParse(Environment.GetEnvironmentVariable("BASE_PORT"), out var basePort) ? basePort : 40000;
    private static readonly int TotalEntryPoints = int.TryParse(Environment.GetEnvironmentVariable("TOTAL_ENTRY_POINTS"), out var basePort) ? basePort : 10;
    private static readonly ConcurrentQueue<TransactionRequest> TransactionQueue = new();
    private static readonly ConcurrentDictionary<int, Client> Clients = new();

    public static void Main()
    {
        SetupClients();
        for (var i = 0; i < WritePipes; i++) new Thread(TransactionWorker).Start();
        for (var i = 0; i < TotalEntryPoints; i++) new Thread(StartServer).Start(MainPort + i);
    }

    private static void StartServer(object? state)
    {
        if (state is not int cliPort) throw new Exception("Error while reading state from Server");
        try
        {
            Console.WriteLine("[{0}] Server started", cliPort);
            var mainListener = TcpListener.Create(cliPort);
            mainListener.ExclusiveAddressUse = true;
            mainListener.Server.NoDelay = true;
            mainListener.Server.Ttl = 255;
            mainListener.Start();
            while (true)    
            {
                var mainCli = mainListener.AcceptTcpClient();
                Console.WriteLine("[{0}] Received call", cliPort);
                var stream = mainCli.GetStream();
                var (parameters, description) = stream.ReadWriteMessage();
                switch (parameters[0])
                {
                    case 0:
                        new Thread(ReadWorker).Start(new ReadRequest(parameters, stream));
                        break;
                    case > 0:
                        new Thread(WriteWorker).Start(new WriteRequest(parameters, description!, stream));
                        break;
                    default:
                        throw new Exception("PANIC!!!");
                }
                Console.WriteLine("[{0}] Available", cliPort);
            }
        }
        catch (Exception e) when (e is SocketException or InvalidOperationException)
        {
            Console.WriteLine(e.Message);
            StartServer(cliPort);
        }
    }
    
    private sealed class ReadRequest(int[] parameters, NetworkStream stream)
    {
        public int[] Parameters { get; init; } = parameters;
        public NetworkStream Stream { get; init; } = stream;
    }
    
    private class WriteRequest(int[] parameters, string description, NetworkStream stream)
    {
        public int[] Parameters { get; init; } = parameters;
        public NetworkStream Stream { get; init; } = stream;
        public string Description { get; set; } = description;
    }
    
    
     #region Workers

     private static void ReadWorker(object? state)
     {
         if (state is not ReadRequest request) throw new ApplicationException("Invalid state while setting up TCP listener");
         _ = Clients.TryGetValue(request.Parameters[1], out var clientData);
         request.Stream.Write(PacketBuilder.WriteMessage([clientData!.Value, clientData.Limit], clientData.Transactions, clientData.FilledLenght));
     }

     private static void WriteWorker(object? state)
     {
         if (state is not WriteRequest request) throw new ApplicationException("Invalid state while setting up TCP listener");
         var transaction = new TransactionRequest(request.Parameters, request.Description);
         TransactionQueue.Enqueue(transaction);
         request.Stream.Write(PacketBuilder.WriteMessage(transaction.AwaitResponse()));
     }
     
     private static void TransactionWorker()
     {
         while (true)
         {
             if (TransactionQueue.IsEmpty) continue;
             if (!TransactionQueue.TryDequeue(out var req))
             {
                 continue;
             }
             var found = Clients.TryGetValue(req.Parameters[0], out var client);
             if (!found)
             {
                 req.Response = [0, 0];
                 continue;
             }
             var value = req.Parameters[1];
             var newBalance = client!.Value + value;
             var isDebit = req.Parameters[1] < 0; 
             if (isDebit && -newBalance > client.Limit)
             {
                 req.Response = [0, -1];
                 continue;
             }
             var transaction = new Transaction(isDebit ? -value : value, isDebit ? 'd' : 'c', req.Description,
                 DateTime.Now.ToString(CultureInfo.InvariantCulture));
             client.SetValue(newBalance);
             client.AddTransaction(transaction);
             req.Response = [newBalance, client.Limit];
         }
     }

     #endregion
     
     private static void SetupClients()
     {
         int[][] clients = [
             [0, 0],
             [0, 100000],
             [0, 80000],
             [0, 1000000],
             [0, 10000000],
             [0, 500000],
         ]; 
         for (var i = 0; i < 6; i++)
         {
             Clients.TryAdd(i, new Client(clients[i][0], clients[i][1]));
         }
     }

    

     

}