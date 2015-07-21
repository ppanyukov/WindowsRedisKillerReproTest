namespace RedisKiller
{
    using System;
    using System.IO;
    using System.Threading.Tasks;

    using StackExchange.Redis;

    public class Program
    {
        public void Main(string[] args)
        {
            // This size of the value will kill Redis on Windows
            // if it's using dlmalloc, even if it's configured to have
            // allkeys-lru eviction policy.
            // Confirmed with latest version of Redis fork on 21 July 2015.
            //
            // Start redis locally with this config:
            //
            //  ./redis-server --maxmemory 134217728 --maxmemory-policy allkeys-lru --appendonly no --save '""'
            //
            // See this SO question for details: http://stackoverflow.com/questions/31525903/redis-aborting-for-out-of-memory/31538609
            //
            var killerArraySize = 64 * 1024;

            try
            {
                ShoveItUpTheRedis(arraySize: killerArraySize, logger: Console.Out).Wait();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"Oh Dear. BOOM. {e.Message}");
            }
        }

        private static async Task ShoveItUpTheRedis(int arraySize, TextWriter logger)
        {
            // Randomly filled array of predefined size
            var array = new byte[arraySize];
            var random = new Random();
            random.NextBytes(array);

            using (var conn = ConnectionMultiplexer.Connect("localhost"))
            {
                var db = conn.GetDatabase(db: 0);

                // Keep shoving this into Redis until it dies
                var iteration = 1;
                var totalBytes = (long)arraySize;
                while (true)
                {
                    var key = $"foo_{iteration.ToString("G")}";
                    logger.WriteLine($"Iteration: {iteration.ToString("N0")}. Total bytes: {(totalBytes * iteration).ToString("N0")}");
                    var result = await db.StringSetAsync(key: key, value: array);
                    iteration++;

                    // Throttle to check if this is to do with client recieve buffers,
                    // perhaps we are sending data too fast?
                    //
                    // But in this case this theory didn't hold.
                    //
                    //await Task.Delay(50);
                    //Thread.Sleep(10);
                }
            }
        }
    }
}
