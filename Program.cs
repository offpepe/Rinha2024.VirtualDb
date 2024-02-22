using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Rinha2024.VirtualDb;

public class Program
{
    private static readonly int range = 10;
    private static readonly TimeSpan defaultWait = TimeSpan.FromTicks(100);
    private static ConcurrentQueue<Request> queue = new();
    private static Dictionary<Guid, int[]> results = new();
    private static int actualPort = 3000;
    
    
    public static void Main()
    {
        Console.WriteLine("Server started");
        var queueThread = new Thread(new ThreadStart(QueueHandlerThread));
        queueThread.Start();
        
        while (true)
        {
            new Thread(new ThreadStart(TryGetRequest)).Start();
        }
    }
    
    private static void QueueHandlerThread()
    {
        var vdb = new VirtualDatabase();
        while (true)
        {
            if (!queue.TryDequeue(out var req))
            {
                Thread.Sleep(defaultWait);
                continue;
            }
            if (req.Read)
            {
                var idx = req.ReadIndex();
                var client = vdb.GetClient(ref idx);
                TryWriteResult(req.OperationId,client);
                continue;
            }
            var parameters = req.ReadTransactionParams();
            if (parameters == null) continue;
            var type = parameters[2] > 0 ? 'c' : 'd';
            TryWriteResult(req.OperationId, vdb.DoTransaction(ref parameters[1], ref type, ref parameters[3]));
        }
    }


    private static void TryGetRequest()
    {
        using var listener = new TcpListener(IPAddress.Any, actualPort);
        listener.Start();
        using var client = listener.AcceptTcpClient();
        using var stream = client.GetStream();
        var buffer = new byte[1024];
        var bytesToRead = stream.Read(buffer);
        if (bytesToRead == 0)
        {
            Console.WriteLine("no data | thread: {0}", Thread.CurrentThread.Name);
            return;
        }

        var parameters = new int[4];
        Buffer.BlockCopy(buffer, 0, parameters, 0, bytesToRead);
        var id = Guid.NewGuid();
        int[] result;
        if (parameters[0] == 0)
        {
            queue.Enqueue(new Request()
            {
                Read = true,
                Parameter = parameters[1],
                OperationId = id
            });
            result = AwaitResponse(id);
            SendData(result, stream);
            Console.WriteLine("READ REQUEST SUCCESS | ID: {0} - RESULT: {1} ", id, string.Join(',', result));
            return;
        }

        queue.Enqueue(new Request()
        {
            Read = false,
            Parameter = parameters,
            OperationId = id
        });
        result = AwaitResponse(id);
        SendData(result, stream);
        Console.WriteLine("WRITE REQUEST SUCCESS | ID: {0} - RESULT: {1} ", id, string.Join(',', result));
    } 
    
    private static int[] AwaitResponse(Guid id)
    {
        var finished = false;
        int[]? result = null;
        while (!finished)
        {
            finished = results.TryGetValue(id, out result);
            if (result != null) break;
        }
        results.Remove(id);
        return result!;
    }

    private static void TryWriteResult(Guid id, int[] data)
    {
        while (true)
        {
            try
            {
                results.Add(id, data);
                break;
            }
            catch (IndexOutOfRangeException _)
            {
                //ignore
            }
        }
    }

    private static void SendData(int[] data, NetworkStream stream)
    {
        for (var i = 0; i < data.Length; i++)
        {
            var buffer = BitConverter.GetBytes(data[i]);
            stream.Write(buffer);
        }
    }


    
    
}