using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Rinha2024.Dotnet.IO;

namespace Rinha2024.VirtualDb;

public class Program
{
    private static readonly int Range =
        int.TryParse(Environment.GetEnvironmentVariable("CONNECTION_RANGE"), out var connectionRange)
            ? connectionRange
            : 1;

    private static readonly int InitialPort =
        int.TryParse(Environment.GetEnvironmentVariable("BASE_PORT"), out var basePort) ? basePort : 7000;

    private static readonly TimeSpan DefaultWait = TimeSpan.FromTicks(10);
    private static readonly ConcurrentQueue<Request> Queue = new();
    private static readonly ConcurrentDictionary<Guid, int[]> Results = new();
    private static readonly ConcurrentQueue<int> ClosedPortsStack = new();
    private static readonly VirtualDatabase Vdb = new();


    public static void Main()
    {
        var queueThread = new Thread(QueueHandlerThread);
        queueThread.Start();
        var deadPortsStack = new Thread(ListenerHandlerProcess);
        deadPortsStack.Start();
        for (var i = 0; i < Range; i++)
        {
            ClosedPortsStack.Enqueue(InitialPort + i);
        }
        Console.WriteLine("Server started");
    }

    private static void ListenerHandlerProcess()
    {
        Console.WriteLine("started to handle processes");
        while (true)
        {
            if (ClosedPortsStack.IsEmpty)
            {
                continue;
            }

            var gotListener = ClosedPortsStack.TryDequeue(out var port);
            if (!gotListener)
            {
                continue;
            }

            var thread = new Thread(TryGetRequest);
            thread.Start(port);
        }
    }

    private static void QueueHandlerThread()
    {
        
        while (true)
        {
            if (!Queue.TryDequeue(out var req))
            {
                continue;
            }

            if (req.Read)
            {
                var idx = req.ReadIndex();
                var client = Vdb.GetClient(ref idx);
                TryWriteResult(req.OperationId, client);
                continue;
            }

            var parameters = req.ReadTransactionParams();
            if (parameters == null)
            {
                TryWriteResult(req.OperationId, [0, 0]);
                continue;
            }

            TryWriteResult(req.OperationId, Vdb.DoTransaction(ref parameters[0], ref parameters[1]));
        }
    }


    private static void TryGetRequest(object? obj)
    {
        if (obj is not int port) throw new ApplicationException("Error while opening TCP listener");
        try
        {
            using var listener = new TcpListener(IPAddress.Any, port);
            listener.ExclusiveAddressUse = true;
            listener.Server.NoDelay = true;
            listener.Start();
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

                result = Vdb.GetClient(ref parameters[1]);
                SendData(result, stream);
#if !ON_CLUSTER
                Console.WriteLine("READ REQUEST SUCCESS | ID: {0} - RESULT: {1} - TUNNEL: {2} ", id,string.Join(',', result), listener.LocalEndpoint);
#endif
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
            Console.WriteLine("WRITE REQUEST SUCCESS | ID: {0} - RESULT: {1} - TUNNEL: {2} ", id,string.Join(',', result), listener.LocalEndpoint);
#endif
            ClosedPortsStack.Enqueue(port);
            listener.Stop();
        }
        catch (IOException _)
        {
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
        var buffer = PacketBuilder.WriteMessage(ref data);
        stream.Write(buffer);
    }
}