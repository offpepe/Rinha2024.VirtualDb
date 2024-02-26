using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using Rinha2024.Dotnet.IO;
using Rinha2024.VirtualDb.IO;

namespace Rinha2024.VirtualDb;

public class Program
{
    private static readonly int MainPort = int.TryParse(Environment.GetEnvironmentVariable("BASE_PORT"), out var basePort) ? basePort : 30000;
    private static readonly ConcurrentQueue<TransactionRequest> TransactionQueue = new();
    private static readonly ConcurrentDictionary<Guid, int[]> Results = new();
    private static readonly ConcurrentDictionary<int, ClientData> Clients = new();

    public static void Main()
    {
        SetupClients();
        var queueThread = new Thread(QueueHandlerThread);
        queueThread.Start();
        for (var i = 0; i < 5; i++)
        {
            new Thread(StartServer).Start(MainPort + i);
        }
    }

    private static void StartServer(object? state)
    {
        if (state is not int cliPort) throw new Exception("Error while reading state from Server");
        try
        {
            Console.WriteLine("[{0}] Server started", cliPort);
            var mainListener = TcpListener.Create(cliPort);
            mainListener.Start();
            var mainCli = mainListener.AcceptTcpClient();
            var stream = mainCli.GetStream();
            while (true)
            {
                var opt = stream.ReadOpt();
                var listener = TcpListener.Create(0);
                listener.Start();
                var port = ((IPEndPoint) listener.LocalEndpoint).Port;
                switch (opt)
                {
                    case 1:
                        new Thread(ReadThread).Start(listener);
                        stream.Write(BitConverter.GetBytes(port));
                        break;
                    case 2:

                        new Thread(WriteThread).Start(listener);
                        stream.Write(BitConverter.GetBytes(port));
                        break;
                    default:
                        throw new Exception("WHAT IS GOING ON????");
                }
            }
        }
        catch (Exception e) when (e is SocketException or InvalidOperationException)
        {
            Console.WriteLine(e.Message);
            StartServer(cliPort);
        }
    }
    
    private static void QueueHandlerThread()
    {
        while (true)
        {
            if (!TransactionQueue.TryDequeue(out var req))
            {
                continue;
            }
            var found = Clients.TryGetValue(req.Parameters[0], out var client);
            if (!found)
            {
                TryWriteResult(req.Id, [0,0]);
                return;
            }
            var value = req.Parameters[1];
            var newBalance = client!.Value + value;
            var isDebit = req.Parameters[1] < 0; 
            if (isDebit && -newBalance > client.Limit)
            {
                TryWriteResult(req.Id, [0,-1]);
                return;
            }
            var transaction = new Transaction(isDebit ? -value : value, isDebit ? 'd' : 'c', req.Description,
                DateTime.Now.ToString(CultureInfo.InvariantCulture));
            client.SetValue(newBalance);
            client.AddTransaction(transaction);
            TryWriteResult(req.Id, [newBalance, client.Limit]);
        }
    }
    
     private static void TryWriteResult(Guid id, int[] data)
     {
         var gotResult = false;
         while (!gotResult)
         {
             try
             {
                 gotResult = Results.TryAdd(id, data);
                 break;
             }
             catch (OverflowException _)
             {
                 //ignore
             }

             Thread.Sleep(TimeSpan.FromTicks(10));
         }
     }

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
             Clients.TryAdd(i, new ClientData(clients[i][0], clients[i][1]));
         }
     }

     private static void ReadThread(object? state)
     {
         if (state is not TcpListener listener) throw new ApplicationException("Invalid state while setting up TCP listener");
         Console.WriteLine("[{0}] Read process opened", listener.LocalEndpoint);
         using var client = listener.AcceptTcpClient();
         using var stream = client.GetStream();
         var parameters = stream.ReadMessage();
         _ = Clients.TryGetValue(parameters[1], out var clientData);
         stream.Write(PacketBuilder.WriteMessage([clientData!.Value, clientData.Limit], clientData.Transactions));
         listener.Stop();
         listener.Dispose();
     }

     private static void WriteThread(object? state)
     {
         if (state is not TcpListener listener) throw new ApplicationException("Invalid state while setting up TCP listener");
         Console.WriteLine("[{0}] Write process opened", listener.LocalEndpoint);
         using var client = listener.AcceptTcpClient();
         using var stream = client.GetStream();
         var id = Guid.NewGuid();
         var (parameters, description) = stream.ReadWriteMessage();
         TransactionQueue.Enqueue(new TransactionRequest(id, parameters, description));
         stream.Write(PacketBuilder.WriteMessage(AwaitResponse(id)));
         listener.Stop();
         listener.Dispose();
     }

     // private static void Listen(object? state)
     // {
     //     if (state is not int port) throw new ApplicationException("Invalid state while setting up TCP listener");
     //     using var listener = TcpListener.Create(port);
     //     listener.Start();
     //     Console.WriteLine("[{0}] Process opened", listener.LocalEndpoint);
     //     using var client = listener.AcceptTcpClient();
     //     using var stream = client.GetStream();
     //     while (true)
     //     {
     //         try
     //         {
     //             var (parameters, description) = stream.ReadWriteMessage();
     //             int[] result;
     //             switch (parameters[0])
     //             {
     //                 case 0:
     //                     _ = Clients.TryGetValue(parameters[1], out var clientData);
     //                     result = [clientData!.Value, clientData.Limit];
     //                     // stream.Write(PacketBuilder.WriteMessage(ref result, clientData.Transactions));
     //                     break;
     //                 default:
     //                     var id = Guid.NewGuid();
     //                     TransactionQueue.Enqueue(new TransactionRequest(id, parameters, description ?? string.Empty));
     //                     result = AwaitResponse(id);
     //                     stream.Write(PacketBuilder.WriteMessage(ref result));
     //                     break;
     //             }
     //         }
     //         catch (Exception e) when (e is SocketException or IOException or InvalidOperationException)
     //         {
     //             DeafQueue.Enqueue(port);
     //             break;
     //         }
     //     }
     // }
     private static int[] AwaitResponse(Guid id)
     {
         var finished = false;
         int[]? result = null;
         while (!finished)
         {
             finished = Results.Remove(id, out result);
             Thread.Sleep(TimeSpan.FromTicks(10));
             if (result != null) break;
         }
         return result!;
     }

     public class ClientData
     {
         public ClientData(int value, int limit)
         {
             Value = value;
             Limit = limit;
         }
         public int Value { get; set; }
         public int Limit { get; set; }
         public LinkedList<Transaction> Transactions { get; set; } = [];
         public void SetValue(int newBalance) => this.Value = newBalance;

         public void AddTransaction(Transaction transaction)
         {
             Transactions.AddFirst(transaction);
             while (Transactions.Count > 10)
             {
                 Transactions.RemoveLast();
             }
         }

         public IEnumerable<Transaction> GetTransactions(int count)
         {
             if (Transactions.Count < count) count = Transactions.Count;
             for (var i = 0; i < count; i++) yield return Transactions.ElementAt(i);
         }
     };
     public readonly record struct Transaction(int Value, char Type, string Description, string CreatedAt);
     public readonly record struct TransactionRequest(Guid Id, int[] Parameters, string Description);

     public readonly record struct SocketMessage(int[] Parameters, string? description);

}