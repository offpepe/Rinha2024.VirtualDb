using System.Collections.Concurrent;
using System.Net.Sockets;
using Rinha2024.Dotnet.IO;

namespace Rinha2024.VirtualDb.Net;



public class ListenerProcess(int port, ConcurrentQueue<Request> transactionQueue, ConcurrentDictionary<Guid, int[]> results, VirtualDatabase vdb, ConcurrentQueue<int> deadPool)
{

    public async Task Watch()
    {
        var listener = TcpListener.Create(port);
        listener.Start();
        Console.WriteLine("[{0}] Process opened", listener.LocalEndpoint);
        var client = await listener.AcceptTcpClientAsync();
        var stream = client.GetStream();
        while (true)
        {
            try
            {
                var operation = await stream.ReadOperationAsync();
                var parameters = await stream.ReadMessageAsync();
                int[] result = [];
                switch (operation)
                {
                    case 0:
                        result = vdb.GetClient(ref parameters[0]);
                        break;
                    case 1:
                        var id = Guid.NewGuid();
                        transactionQueue.Enqueue(new Request(parameters, id));
                        result = AwaitResponse(id);
                        break;
                    default:
                        Console.WriteLine("SOMETHING IS WRONG!!!!");
                        break;
                }
                stream.Write(PacketBuilder.WriteMessage(ref result));
            }
            catch (Exception e) when (e is SocketException or IOException or InvalidOperationException)
            {
                deadPool.Enqueue(port);
                break;
            }
        }
    }
    
    
    private int[] AwaitResponse(Guid id)
    {
        var finished = false;
        int[]? result = null;
        while (!finished)
        {
            finished = results.Remove(id, out result);
            Thread.Sleep(TimeSpan.FromTicks(10));
            if (result != null) break;
        }
        return result!;
    }
    
    
}