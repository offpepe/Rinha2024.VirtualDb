using System.Collections.Concurrent;
using System.Globalization;
using System.Net.Sockets;
using Rinha2024.VirtualDb.Extensions;
using Rinha2024.VirtualDb.IO;

namespace Rinha2024.VirtualDb;

public class Program
{
    private static readonly int WritePipes = int.TryParse(Environment.GetEnvironmentVariable("WRITE_PIPES"), out var writePipes) ? writePipes : 1;
    private static readonly int WPort = int.TryParse(Environment.GetEnvironmentVariable("W_BASE_PORT"), out var basePort) ? basePort : 40000;
    private static readonly int RPort = int.TryParse(Environment.GetEnvironmentVariable("R_BASE_PORT"), out var basePort) ? basePort : 20000;
    private static readonly int ListenerNum = int.TryParse(Environment.GetEnvironmentVariable("LISTENERS"), out var listeners) ? listeners : 20;
    private static readonly ConcurrentQueue<TransactionRequest> TransactionQueue = new();
    private static readonly ConcurrentDictionary<int, Client> Clients = new();


    public static void Main()
    {
        SetupClients();
        for (var i = 0; i < WritePipes; i++) new Thread(TransactionWorker).Start();
        for (var i = 0; i < ListenerNum; i++)
        {
            new Thread(ReadChannel).Start(RPort + i);
            new Thread(WriteChannel).Start(WPort + i);
        }
    }
    
    private static void ReadChannel(object? state)
    {
        if (state is not int cliPort) throw new Exception("Error while reading state from Server");
        Console.WriteLine("[R::{0}] Server started", cliPort);
        var mainListener = TcpListener.Create(cliPort);
        mainListener.Configure();
        mainListener.Start();
        while (true)
        {
            var mainCli = mainListener.AcceptTcpClient();
            var stream = mainCli.GetStream();
            var parameters = stream.ReadMessage();
            _ = Clients.TryGetValue(parameters[1], out var clientData);
            stream.Write(PacketBuilder.WriteMessage([clientData!.Value, clientData.Limit], clientData.Transactions, clientData.FilledLenght));
        }
    }
    
    private static void WriteChannel(object? state)
    {
        if (state is not int cliPort) throw new Exception("Error while reading state from Server");
        Console.WriteLine("[W::{0}] Server started", cliPort);
        var mainListener = TcpListener.Create(cliPort);
        mainListener.Configure();
        mainListener.Start();
        while (true)
        {
            var mainCli = mainListener.AcceptTcpClient();
            var stream = mainCli.GetStream();
            var (parameters, description) = stream.ReadWriteMessage();
            var transaction = new TransactionRequest(parameters, description, stream);
            TransactionQueue.Enqueue(transaction);
        }
    }
    
    
     #region Workers
     private static void TransactionWorker()
     {
         while (true)
         {
             if (TransactionQueue.IsEmpty || !TransactionQueue.TryDequeue(out var req))
             {
                 Thread.Sleep(1);
                 continue;
             }
             _ = Clients.TryGetValue(req.Parameters[0], out var client);
             var value = req.Parameters[1];
             var newBalance = client!.Value + value;
             var isDebit = req.Parameters[1] < 0; 
             if (isDebit && -newBalance > client.Limit)
             {
                 req.SendResponse([0, -1]);
                 continue;
             }
             var transaction = new Transaction(isDebit ? -value : value, isDebit ? 'd' : 'c', req.Description,
                 DateTime.Now.ToString(CultureInfo.InvariantCulture));
             client.SetValue(newBalance);
             client.AddTransaction(transaction);
             req.SendResponse([newBalance, client.Limit]);
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