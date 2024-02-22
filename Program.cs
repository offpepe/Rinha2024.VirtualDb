using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Rinha2024.VirtualDb;

public class Program
{
    private const int RANGE = 1000;
    private const int INITIAL_PORT = 6000;
    private static readonly TimeSpan DefaultWait = TimeSpan.FromTicks(100);
    private static readonly ConcurrentQueue<Request> Queue = new();
    private static readonly ConcurrentDictionary<Guid, int[]> Results = new();
    
    
    public static void Main()
    {
        Console.WriteLine("Server started");
        var queueThread = new Thread(new ThreadStart(QueueHandlerThread));
        queueThread.Start();
        for (var i = 0; i < RANGE; i++)
        {
            var thread = new Thread(new ParameterizedThreadStart(TryGetRequest));
            thread.Start(new TcpListener(IPAddress.Any, INITIAL_PORT + i));
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
                TryWriteResult(req.OperationId,client);
                continue;
            }

            var parameters = req.ReadTransactionParams();
            if (parameters == null)
            {
                TryWriteResult(req.OperationId, [0,0]);
                continue;
            }
            var type = parameters[1] > 0 ? 'c' : 'd';
            TryWriteResult(req.OperationId, vdb.DoTransaction(ref parameters[0], ref type, ref parameters[1]));
        }
    }
    

    private static void TryGetRequest(object? obj)
    {
        if (obj is not TcpListener listener) throw new ApplicationException("Error while opening TCP listener");
        Console.WriteLine("listening on {0}", listener.LocalEndpoint);
        listener.Start();
        var client = listener.AcceptTcpClient();
        while (true)
        {
            if (!client.Connected)
            {
                client = listener.AcceptTcpClient();
            }
            var stream = client.GetStream();
            var parameters = new int[2];
            var buffer = new byte[16];
            var bytes = stream.Read(buffer);
            parameters[0] = BitConverter.ToInt32(buffer);
            bytes =  stream.Read(buffer);
            parameters[1] = BitConverter.ToInt32(buffer);
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
                Console.WriteLine("READ REQUEST SUCCESS | ID: {0} - RESULT: {1} - TUNNEL: {2} ", id, string.Join(',', result), listener.LocalEndpoint);
                continue;
            }

            Queue.Enqueue(new Request()
            {
                Read = false,
                Parameter = parameters,
                OperationId = id
            });
            try
            {
                result = AwaitResponse(id);
                SendData(result, stream);
            }
            catch
            {
                Console.WriteLine("Error while reading stream | ID: {0} - TUNNEL: {1} ", id, listener.LocalEndpoint);
                continue;
            }
            Console.WriteLine("WRITE REQUEST SUCCESS | ID: {0} - RESULT: {1} - TUNNEL: {2} ", id, string.Join(',', result), listener.LocalEndpoint);
        }
    } 
    
    private static int[] AwaitResponse(Guid id)
    {
        var finished = false;
        int[]? result = null;
        while (!finished)
        {
            finished = Results.Remove(id, out result);
            if (result != null) break;
        }
        return result!;
    }

    private static void TryWriteResult(Guid id, int[] data)
    {
        while (true)
        {
            try
            {
                Results.TryAdd(id, data);
                break;
            }
            catch (OverflowException _)
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