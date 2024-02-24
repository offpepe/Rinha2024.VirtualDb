using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Rinha2024.Dotnet.IO;
using Rinha2024.VirtualDb.Net;

namespace Rinha2024.VirtualDb;

public class Program
{
    private static readonly int Range =
        int.TryParse(Environment.GetEnvironmentVariable("CONNECTION_RANGE"), out var connectionRange)
            ? connectionRange
            : 5000;

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
            var process = new ListenerProcess(InitialPort + i, Queue, Results, Vdb, ClosedPortsStack);
            Task.Run(async () => await process.Watch());
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
            var process = new ListenerProcess(port, Queue, Results, Vdb, ClosedPortsStack);
            Task.Run(async () => await process.Watch());
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
            TryWriteResult(req.OperationId, Vdb.DoTransaction(ref req.Parameters[0], ref req.Parameters[1]));
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
}