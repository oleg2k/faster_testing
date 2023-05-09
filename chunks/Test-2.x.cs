using FASTER.core;
using System.Buffers.Binary;
using System.Text;

namespace ConsoleApp74;

using MySession = ClientSession<SpanByte, SpanByte, SpanByte, bool, Empty, ReducedUserDataFunctions>;

internal class Program
{
    static void Main(string[] args)
    {
        int? pSecondaryIndexCnt = null;

        if (args.Length > 0)
        {
            if (int.TryParse(args[0], out var v))
                pSecondaryIndexCnt = v;
        }

        for (var i = 0; i < 100; i++)
        {
            var dbPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"faster-{i}");
            try
            {
                var secondaryIndexCnt = pSecondaryIndexCnt ?? DefaultSecondaryIndexCnt; // 3 + random.Next(8);

                Write(dbPath, secondaryIndexCnt);
            }
            finally
            {
                try
                {
                    if (Directory.Exists(dbPath))
                        Directory.Delete(dbPath, true);
                }
                catch
                {
                    // ignore
                }
            }
        }
    }

    private static readonly Random random = new(1);

    private const int ValueSize = 1024;
    private const int DefaultSecondaryIndexCnt = 5;

    private const int StaticStringArrayLength = 16 * 1024;
    private static readonly string?[] staticStringArray = new string?[StaticStringArrayLength];

    private static void Write(string dbPath, int secondaryIndexCnt)
    {
        Console.WriteLine(dbPath);

        if (Directory.Exists(dbPath))
            Directory.Delete(dbPath, true);

        var mainDbPath = Path.Combine(dbPath, "main");

        var tempIndexDbDirectory = Path.Combine(dbPath, "TempIndex");

        using var mainLogDevice = Devices.CreateLogDevice(mainDbPath);
        using var mainKvSettings = new FasterKVSettings<SpanByte, SpanByte> { LogDevice = mainLogDevice, PageSize = 1 * 1024 * 1024, SegmentSize = 32 * 1024 * 1024, CheckpointDir = mainDbPath };
        using var mainKv = new FasterKV<SpanByte, SpanByte>(mainKvSettings);

        var indexDevices = new List<IDevice>();
        var indexSettings = new List<FasterKVSettings<SpanByte, SpanByte>>();
        var indexKvs = new List<FasterKV<SpanByte, SpanByte>>();

        for (var i = 0; i < secondaryIndexCnt; i++)
        {
            var indexPath = Path.Combine(tempIndexDbDirectory, $"i_{i}");

            var indexLogDevice = Devices.CreateLogDevice(indexPath);
            var indexKvSettings = new FasterKVSettings<SpanByte, SpanByte> { LogDevice = indexLogDevice, PageSize = 1 * 1024 * 1024, SegmentSize = 32 * 1024 * 1024, CheckpointDir = indexPath };
            var indexKv = new FasterKV<SpanByte, SpanByte>(indexKvSettings);

            indexDevices.Add(indexLogDevice);
            indexSettings.Add(indexKvSettings);
            indexKvs.Add(indexKv);
        }

        {
            using var mainKvSession = mainKv.For(ReducedUserDataFunctions.Instance).NewSession<ReducedUserDataFunctions>();

            var indexSessions = new List<MySession>();

            foreach (var indexKv in indexKvs)
            {
                indexSessions.Add(indexKv.For(ReducedUserDataFunctions.Instance).NewSession<ReducedUserDataFunctions>());
            }

            var recordCounter = 0;

            Span<byte> primaryKeyFixedBuffer = stackalloc byte[8];
            Span<byte> valueFixedBuffer = stackalloc byte[ValueSize];

            for (long key = 0; key < 500_000; key++)
            {
                recordCounter++;

                random.NextBytes(valueFixedBuffer);

                BinaryPrimitives.WriteInt64BigEndian(primaryKeyFixedBuffer, key);

                var secondaryKeySpans = new List<SpanByte>();

                for (var i = 0; i < secondaryIndexCnt; i++)
                {
                    secondaryKeySpans.Add(SpanByte.FromFixedSpan(valueFixedBuffer.Slice(i * 4, 4)));
                }

                var primaryKeySpan = SpanByte.FromFixedSpan(primaryKeyFixedBuffer);

                Write(mainKvSession, primaryKeySpan, SpanByte.FromFixedSpan(valueFixedBuffer));

                for (var i = 0; i < secondaryIndexCnt; i++)
                {
                    Write(indexSessions[i], secondaryKeySpans[i], primaryKeySpan);
                }

                // feed GC

                for (var i = 0; i < 500; i++)
                {
                    new StringBuilder(100);

                    var pos = random.Next(StaticStringArrayLength);

                    if (random.Next(2) == 1)
                        staticStringArray[pos] = new string('r', random.Next(128));
                    else
                        staticStringArray[pos] = null;
                }

                if (recordCounter % 10_000 == 0)
                {
                    Console.WriteLine($"{recordCounter}...");
                }
            }

            Console.WriteLine($"{recordCounter} done");

            foreach (var indexSession in indexSessions) indexSession.Dispose();
        }

        foreach (var indexKv in indexKvs) indexKv.Dispose();
        foreach (var indexSetting in indexSettings) indexSetting.Dispose();
        foreach (var indexDevice in indexDevices) indexDevice.Dispose();
    }

    private static void Write(MySession session, SpanByte key, SpanByte value)
    {
        var status = session.Upsert(key, value);

        if (status.IsPending)
        {
            session.CompletePending(true);
        }
    }
}

internal sealed class ReducedUserDataFunctions : SpanByteFunctions<SpanByte, bool, Empty>
{
    public static readonly ReducedUserDataFunctions Instance = new();

    private ReducedUserDataFunctions()
    {
    }
}
