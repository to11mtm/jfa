using System.Runtime.CompilerServices;

namespace ConsoleApp1;

public class Wat
{
    public async Task Derp()
    {
        Console.WriteLine("Hit X to stop");
        using var cts = new CancellationTokenSource();
        var resource = new Resource();
        Task.Run(async () =>
        {
            await Task.Yield();
            var w = Generator.DoAsyncEnumerable(resource,default).GetAsyncEnumerator(cts.Token);
            while (await w.MoveNextAsync())
            {
                Console.WriteLine(w.Current);
            }
            //await foreach (var x in w)
            //{
            //    Console.WriteLine(x);
            //}
        });
        while (cts.IsCancellationRequested == false)
        {
            var res = Console.ReadLine();
            if (res?.Equals("x", StringComparison.InvariantCultureIgnoreCase) ==
                true)
            {
                cts.Cancel();
            }
        }

        await Task.Delay(500);
        Console.WriteLine($"After waiting, resource {(resource.IsActive? "was not" : "was")} disposed");
    }
    
    


}

public static class Generator
{
    public static async  IAsyncEnumerable<int> DoAsyncEnumerable(Resource resource,[EnumeratorCancellation]CancellationToken token)
    {
        await using var res = resource;
        int i = 0;
        while (true)
        {
            await Task.Delay(1000, token);
            yield return i++;
        }
    }
}
public class Resource : IAsyncDisposable
{
    public bool IsActive = true;
    public ValueTask DisposeAsync()
    {
        IsActive = false;
        Console.WriteLine("Enumerator completed and resource disposed");
        return ValueTask.CompletedTask;
    }
}