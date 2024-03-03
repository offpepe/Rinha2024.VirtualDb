using System.Collections.Concurrent;
using System.Globalization;
using System.Net.Sockets;
using System.Text;
using Rinha2024.VirtualDb.Extensions;
using Rinha2024.VirtualDb.IO;

namespace Rinha2024.VirtualDb;

public class Program
{
    private static readonly int WritePipes = int.TryParse(Environment.GetEnvironmentVariable("WRITE_PIPES"), out var writePipes) ? writePipes : 1;
    private static readonly int WPort = int.TryParse(Environment.GetEnvironmentVariable("W_BASE_PORT"), out var basePort) ? basePort : 10000;
    private static readonly int RPort = int.TryParse(Environment.GetEnvironmentVariable("R_BASE_PORT"), out var basePort) ? basePort : 15000;
    private static readonly int ListenerNum = int.TryParse(Environment.GetEnvironmentVariable("LISTENERS"), out var listeners) ? listeners : 20;
    private static readonly ConcurrentDictionary<int, Client> Clients = new();


    public static void Main()
    {
        var storageFolder = Environment.GetEnvironmentVariable("storage_folder") ?? "./data";
        if (!Directory.Exists(storageFolder)) Directory.CreateDirectory(storageFolder);
        SetupClients();
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
            _ = Clients.TryGetValue(parameters[0], out var clientData);
            stream.Write(PacketBuilder.WriteMessage(clientData!.DoTransaction(parameters[1], description)));
        }
    }
     private static void SetupClients()
     {
         int[][] clients = [
             [0, 100000],
             [0, 80000],
             [0, 1000000],
             [0, 10000000],
             [0, 500000],
         ]; 
         for (var i = 0; i < 5; i++)
         {
             var client = new Client(clients[i][0], clients[i][1], i + 1);
             Clients.TryAdd(i + 1, client);
         }
     }


}