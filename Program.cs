using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Reflection.Metadata;
using Rinha2024.VirtualDb.Extensions;
using Rinha2024.VirtualDb.IO;
using Guid = System.Guid;

namespace Rinha2024.VirtualDb;

public class Program
{
    private static readonly int MainPort = int.TryParse(Environment.GetEnvironmentVariable("BASE_PORT"), out var basePort) ? basePort : 20000;
    private static readonly ConcurrentQueue<TransactionRequest> TransactionQueue = new();
    private static readonly ConcurrentDictionary<Guid, int[]> Results = new();
    private static readonly ConcurrentDictionary<int, Client> Clients = new();

    public static void Main()
    {
        SetupClients();
        var queueThread = new Thread(TransactionWorker)
        {
            IsBackground = true
        };
        queueThread.Start();
        for (var i = 0; i < 1000; i++)
        {
            new Thread(Listener).Start(MainPort + i);
        }
    }

    private static void Listener(object? state)
    {
        if (state is not int cliPort) throw new Exception("Error while reading state from Server");
        try
        {
            Console.WriteLine("[{0}] Server started", cliPort);
            var mainListener = TcpListener.Create(cliPort);
            mainListener.Server.NoDelay = true;
            mainListener.Server.Ttl = 255;
            mainListener.ExclusiveAddressUse = true;
            mainListener.Start();
            var mainCli = mainListener.AcceptTcpClient();
            var stream = mainCli.GetStream();
            var needsAcceptance = false;
            while (true)    
            {
                if (needsAcceptance)
                {
                    stream.Close();
                    mainCli.Close();
                    mainListener.Stop();
                    mainListener = TcpListener.Create(cliPort);
                    mainListener.Server.NoDelay = true;
                    mainListener.Server.Ttl = 255;
                    mainListener.ExclusiveAddressUse = true;
                    mainListener.Start();
                    Console.WriteLine("\n[M::{0}]-REVIVED", cliPort);
                    mainCli = mainListener.AcceptTcpClient();
                    stream = mainCli.GetStream();
                    needsAcceptance = false;
                }
                byte opt = 0;
                try
                {
                    opt = stream.ReadOpt();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    if (!mainCli.Connected)
                    {
                        needsAcceptance = true;
                        continue;
                    }
                }
                TcpListener listener;
                listener = TcpListener.Create(0);
                listener.Start();
                var port = ((IPEndPoint) listener.LocalEndpoint).Port;
                switch (opt)
                {
                    case 1:
                        new Thread(ReadWorker).Start(listener);
                        stream.Write(BitConverter.GetBytes(port));
                        break;
                    case 2:

                        new Thread(WriteWorker).Start(listener);
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
            Listener(cliPort);
        }
    }
    
     #region Workers

     private static void ReadWorker(object? state)
     {
         if (state is not TcpListener listener) throw new ApplicationException("Invalid state while setting up TCP listener");
         Console.Write("--[R{0}]", listener.LocalEndpoint);
         using var client = listener.AcceptTcpClient();
         client.ReceiveTimeout = 10000;
         var stream = client.GetStream();
         var parameters = stream.ReadMessage();
         _ = Clients.TryGetValue(parameters[1], out var clientData);
         stream.Write(PacketBuilder.WriteMessage([clientData!.Value, clientData.Limit], clientData.Transactions, clientData.FilledLenght));
         listener.Stop();
     }

     private static void WriteWorker(object? state)
     {
         if (state is not TcpListener listener) throw new ApplicationException("Invalid state while setting up TCP listener");
         Console.Write("--[W{0}]", listener.LocalEndpoint);
         using var client = listener.AcceptTcpClient();
         client.ReceiveTimeout = 10000;
         using var stream = client.GetStream();
         var id = Guid.NewGuid();
         var (parameters, description) = stream.ReadWriteMessage();
         TransactionQueue.Enqueue(new TransactionRequest(id, parameters, description));
         stream.Write(PacketBuilder.WriteMessage(id.AwaitResponse(Results)));
         listener.Stop();
     }
     
     private static void TransactionWorker()
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
                 Results.TryWriteResult(req.Id, [0,0]);
                 return;
             }
             var value = req.Parameters[1];
             var newBalance = client!.Value + value;
             var isDebit = req.Parameters[1] < 0; 
             if (isDebit && -newBalance > client.Limit)
             {
                 Results.TryWriteResult(req.Id, [0,-1]);
                 return;
             }
             var transaction = new Transaction(isDebit ? -value : value, isDebit ? 'd' : 'c', req.Description,
                 DateTime.Now.ToString(CultureInfo.InvariantCulture));
             client.SetValue(newBalance);
             client.AddTransaction(transaction);
             Results.TryWriteResult(req.Id, [newBalance, client.Limit]);
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