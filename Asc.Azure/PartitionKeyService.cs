using ConsistentSharp;

namespace Asc.Azure
{
    public sealed class PartitionKeyService
    {
        private static readonly int TOTAL_PARTITIONS = 1000;
        private static readonly ConsistentHash consistentHash = new ConsistentHash();

        static PartitionKeyService()
        {
            for (var i = 0; i < TOTAL_PARTITIONS; i++)
            {
                consistentHash.Add($"p{i}");
            }
        }

        public static string Get(string name)
        {
            return consistentHash.Get(name);
        }
    }
}
