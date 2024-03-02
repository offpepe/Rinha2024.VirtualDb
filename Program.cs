using System.Collections.Concurrent;
using System.Globalization;
using System.Net.Sockets;
using Rinha2024.VirtualDb.Extensions;
using Rinha2024.VirtualDb.IO;

namespace Rinha2024.VirtualDb;

public class Program
{
    private static readonly int WritePipes = int.TryParse(Environment.GetEnvironmentVariable("WRITE_PIPES"), out var writePipes) ? writePipes : 1;
    private static readonly int WPort = int.TryParse(Environment.GetEnvironmentVariable("W_BASE_PORT"), out var basePort) ? basePort : 10000;
    private static readonly int RPort = int.TryParse(Environment.GetEnvironmentVariable("R_BASE_PORT"), out var basePort) ? basePort : 15000;
    private static readonly int ListenerNum = int.TryParse(Environment.GetEnvironmentVariable("LISTENERS"), out var listeners) ? listeners : 1;
    private static readonly ConcurrentQueue<TransactionRequest> TransactionQueue = new();
    private static readonly ConcurrentDictionary<int, Client> Clients = new();
    private static readonly ConcurrentDictionary<Guid, PersistClientInfo> ClientPersistency = new();
    private static readonly ConcurrentQueue<Transaction> TransactionPersistencyQueue = new();


    public static void Main()
    {
        SetupClients();
        // for (var i = 0; i < WritePipes; i++) new Thread(TransactionWorker).Start();
        // new Thread(PersistClientData).Start();
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

    private static void DoTransaction(object? state)
    {
        if (state is not TcpClient client) throw new Exception("Error while reading state from Server");
        var stream = client.GetStream();
        var (parameters, description) = stream.ReadWriteMessage();
        _ = Clients.TryGetValue(parameters[0], out var clientData);
        stream.Write(PacketBuilder.WriteMessage(clientData!.DoTransaction(parameters[1], description)));
    }

    private static void GetClientPersistedData(int id,Client client)
    {
        var path = $"./data/{id}.capv";
        if (File.Exists(path))
        {
            VerifyClientData(path, client, id);
            return;
        }
        Directory.CreateDirectory("./data");
        using var clientFile = File.Create(path);
        using var transactionFiles = File.Create($"./data/{id}-transactions.capv");
        transactionFiles.Close();
        var writeBuffer = new byte[8];
        var limitBytes = BitConverter.GetBytes(client.Limit);
        writeBuffer[0] = limitBytes[0];
        writeBuffer[1] = limitBytes[1];
        writeBuffer[2] = limitBytes[2];
        writeBuffer[3] = limitBytes[3];
        var valueBytes = BitConverter.GetBytes(client.Value);
        writeBuffer[4] = valueBytes[0];
        writeBuffer[5] = valueBytes[1];
        writeBuffer[6] = valueBytes[2];
        writeBuffer[7] = valueBytes[3];
        clientFile.Write(writeBuffer);
    }

    private static void VerifyClientData(string path, Client client, int id)
    {
        using var file = File.OpenRead(path);
        if (file.Length != 8)
        {
            GetClientPersistedData(id, client);
            return;
        }
        var buffer = new byte[8];   
        _ = file.Read(buffer);
        client.Limit = BitConverter.ToInt32(buffer, 0);
        client.Value = BitConverter.ToInt32(buffer, 4);
    }

    private static void PersistClientData()
    {
        foreach (var client in ClientPersistency)
        {
            var (uuid, data) = client;
            if (!data.Changed) continue;
            using var clientFile = File.OpenWrite($"./data/{uuid}.capv");
            var writeBuffer = new byte[4];
            var position = 4;
            var newValue = BitConverter.GetBytes(data.Value);
            writeBuffer[position] = newValue[0];
            position++;
            writeBuffer[position] = newValue[1];
            position++;
            writeBuffer[position] = newValue[2];
            position++;
            writeBuffer[position] = newValue[3];
            clientFile.Write(writeBuffer, 4, 4);
        }
        Thread.Sleep(TimeSpan.FromSeconds(3));
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
             GetClientPersistedData(i + 1, client);
             Clients.TryAdd(i + 1, client);
         }
     }

     private readonly record struct PersistClientInfo(bool Changed, int Id, int Value);





}