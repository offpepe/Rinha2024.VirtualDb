using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Rinha2024.Dotnet.IO;

namespace Rinha2024.VirtualDb;

public class Program
{
    private static readonly int Range = int.TryParse(Environment.GetEnvironmentVariable("CONNECTION_RANGE"), out var connectionRange) ? connectionRange : 500;
    private static readonly int InitialPort = int.TryParse(Environment.GetEnvironmentVariable("BASE_PORT"), out var basePort) ? basePort : 7000;
    private static readonly TimeSpan DefaultWait = TimeSpan.FromTicks(100);
    private static readonly ConcurrentQueue<Request> Queue = new();
    private static readonly ConcurrentDictionary<Guid, int[]> Results = new();
    private static readonly ConcurrentQueue<int> ClosedPortsStack = new();


    public static void Main()
    {
        Console.WriteLine("Server started");
        var queueThread = new Thread(new ThreadStart(QueueHandlerThread));
        queueThread.Start();
        var deadPortsStack = new Thread(new ThreadStart(ListenerHandlerProcess));
        deadPortsStack.Start();
        
        for (var i = 0; i < Range; i++)
        {
            var thread = new Thread(new ParameterizedThreadStart(TryGetRequest));
            thread.Start();
        }
        
    }

    private static void ListenerHandlerProcess()
    {
        Console.WriteLine("started to handle processes");
        while (true)
        {
            if (ClosedPortsStack.IsEmpty)
            {
                Thread.Sleep(TimeSpan.FromTicks(300));
                continue;
            }

            var gotListener = ClosedPortsStack.TryDequeue(out var port);
            if (!gotListener)
            {
                Thread.Sleep(TimeSpan.FromTicks(300));
                continue;
            }

            var thread = new Thread(new ParameterizedThreadStart(TryGetRequest));
            thread.Start(port);
        }
    }

    private static void QueueHandlerThread()
    {
        var vdb = new VirtualDatabase();
        while (true)
        {
            if (!Queue.TryDequeue(out var req))
            {
                Thread.Sleep(DefaultWait);
                continue;
            }

            if (req.Read)
            {
                var idx = req.ReadIndex();
                var client = vdb.GetClient(ref idx);
                TryWriteResult(req.OperationId, client);
                continue;
            }

            var parameters = req.ReadTransactionParams();
            if (parameters == null)
            {
                TryWriteResult(req.OperationId, [0, 0]);
                continue;
            }

            var type = parameters[1] > 0 ? 'c' : 'd';
            TryWriteResult(req.OperationId, vdb.DoTransaction(ref parameters[0], ref type, ref parameters[1]));
        }
    }


    private static void TryGetRequest(object? obj)
    {
        if (obj is not int port) throw new ApplicationException("Error while opening TCP listener");
        using var listener = new TcpListener(IPAddress.Any, port);
        listener.ExclusiveAddressUse = true;
        listener.Server.NoDelay = true;
        listener.Start();
        try
        {   
#if !ON_CLUSTER 
            Console.WriteLine("process {0} is available", listener.LocalEndpoint);
#endif
            using var client = listener.AcceptTcpClient();
#if !ON_CLUSTER 
            Console.WriteLine("process {0} got request", listener.LocalEndpoint);
#endif
            using var stream = client.GetStream();
            var parameters = stream.ReadMessage();
            var id = Guid.NewGuid();
            int[] result;
            if (parameters[0] == 0)
            {
                Queue.Enqueue(new Request()
                {
                    Read = true,
                    Parameter = parameters[1],
                    OperationId = id
                });
                result = AwaitResponse(id);
                SendData(result, stream);
#if !ON_CLUSTER 
                Console.WriteLine("READ REQUEST SUCCESS | ID: {0} - RESULT: {1} - TUNNEL: {2} ", id,
#endif
                    string.Join(',', result), listener.LocalEndpoint);
                stream.Close();
                listener.Stop();
                ClosedPortsStack.Enqueue(port);
                return;
            }

            Queue.Enqueue(new Request()
            {
                Read = false,
                Parameter = parameters,
                OperationId = id
            });
            result = AwaitResponse(id);
            SendData(result, stream);
#if !ON_CLUSTER 
            Console.WriteLine("WRITE REQUEST SUCCESS | ID: {0} - RESULT: {1} - TUNNEL: {2} ", id,
#endif
                string.Join(',', result),
                listener.LocalEndpoint);
            ClosedPortsStack.Enqueue(port);
            stream.Close();
            listener.Stop();
        }
        catch (IOException _)
        {
            listener.Stop();
            ClosedPortsStack.Enqueue(port);
        }
    }

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

    private static void SendData(int[] data, NetworkStream stream)
    {
        var buffer = PacketBuilder.WriteMessage(data);
        stream.Write(buffer);
    }
    

}