using System.Collections.Concurrent;

namespace Rinha2024.VirtualDb.Extensions;

public static class DictionaryExtensions
{
    public static void TryWriteResult(this ConcurrentDictionary<Guid,int[]> results, Guid id, int[] data)
    {
        while (true)
        {
            try
            {
                results.TryAdd(id, data);
                break;
            }
            catch (OverflowException _) { /*ignore */ }
            Thread.Sleep(TimeSpan.FromTicks(10));
        }
    }
}