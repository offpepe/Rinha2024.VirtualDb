using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Rinha2024.VirtualDb.Extensions;
using Rinha2024.VirtualDb.IO;
using Guid = System.Guid;

namespace Rinha2024.VirtualDb;

public class Program
{
    private static readonly int WritePipes = int.TryParse(Environment.GetEnvironmentVariable("WRITE_PIPES"), out var writePipes) ? writePipes : 2;
    private static readonly int MainPort = int.TryParse(Environment.GetEnvironmentVariable("BASE_PORT"), out var basePort) ? basePort : 40000;
    private static readonly int TotalEntryPoints = int.TryParse(Environment.GetEnvironmentVariable("TOTAL_ENTRY_POINTS"), out var basePort) ? basePort : 50;
    private static readonly ConcurrentQueue<TransactionRequest> TransactionQueue = new();
    private static readonly ConcurrentDictionary<Guid, int[]> ResultPool = new();
    private static readonly ConcurrentDictionary<int, Client> Clients = new();

    public static void Main()
    {
        SetupClients();
        for (var i = 0; i < WritePipes; i++)
        {
            new Thread(TransactionWorker).Start();
        }
        for (var i = 0; i < TotalEntryPoints; i++)
        {
            new Thread(StartServer).Start(MainPort + i);
        }
    }

    private static void StartServer(object? state)
    {
        if (state is not int cliPort) throw new Exception("Error while reading state from Server");
        try
        {
            // Console.WriteLine("[{0}] Server started", cliPort);
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
                    // Console.WriteLine("\n[M::{0}]-REVIVED", cliPort);
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
                    // Console.WriteLine(e.GetType());
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
            StartServer(cliPort);
        }
    }
    
     #region Workers

     private static void ReadWorker(object? state)
     {
         if (state is not TcpListener listener) throw new ApplicationException("Invalid state while setting up TCP listener");
         // Console.Write("--[R{0}]", listener.LocalEndpoint);
         listener.Server.ReceiveTimeout = 10000;
         using var client = listener.AcceptTcpClient();
         var stream = client.GetStream();
         var parameters = stream.ReadMessage();
         _ = Clients.TryGetValue(parameters[1], out var clientData);
         stream.Write(PacketBuilder.WriteMessage([clientData!.Value, clientData.Limit], clientData.Transactions, clientData.FilledLenght));
         listener.Stop();
     }

     private static void WriteWorker(object? state)
     {
         if (state is not TcpListener listener) throw new ApplicationException("Invalid state while setting up TCP listener");
         // Console.Write("--[W{0}]", listener.LocalEndpoint);
         listener.Server.ReceiveTimeout = 10000;
         using var client = listener.AcceptTcpClient();
         using var stream = client.GetStream();
         var id = Guid.NewGuid();
         var (parameters, description) = stream.ReadWriteMessage();
         var transaction = new TransactionRequest(id, parameters, description);
         TransactionQueue.Enqueue(transaction);
         stream.Write(PacketBuilder.WriteMessage(transaction.AwaitResponse()));
         listener.Stop();
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
                 return;
             }
             var value = req.Parameters[1];
             var newBalance = client!.Value + value;
             var isDebit = req.Parameters[1] < 0; 
             if (isDebit && -newBalance > client.Limit)
             {
                 req.Response = [0, -1];
                 return;
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