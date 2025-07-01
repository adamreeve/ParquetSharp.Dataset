using ParquetSharp;
using ParquetSharp.Arrow;
using ParquetSharp.Dataset;

namespace Benchmark;

class Program
{
    static async Task Main(string[] args)
    {
        await ReadDataset(args[0], dateFilter: false, idFilter: true);
        //await ReadWithArrow($"{args[0]}/test_data.parquet");
        //await ReadWithPqs($"{args[0]}/test_data.parquet");
    }

    static async Task ReadDataset(string directoryPath, bool dateFilter = false, bool idFilter = false)
    {
        using var arrowProperties = ArrowReaderProperties.GetDefault();
        //arrowProperties.PreBuffer = false;
        var reader = new DatasetReader(directoryPath, arrowReaderProperties: arrowProperties);
        IFilter? filter = null;
        if (dateFilter)
        {
            filter = Col.Named("date").IsEqualTo(new DateOnly(2025, 1, 10));
        }

        if (idFilter)
        {
            var filterId = Col.Named("id").IsEqualTo(3);
            filter = filter == null ? filterId : filter.And(filterId);
        }

        using var stream = reader.ToBatches(filter: filter);
        var batchCount = 0;
        while (await stream.ReadNextRecordBatchAsync() is { } batch)
        {
            batchCount++;
            batch.Dispose();
        }

        Console.WriteLine($"Read {batchCount} batches");
    }

    static async Task ReadWithArrow(string filePath)
    {
        using var arrowProps = ArrowReaderProperties.GetDefault();
        arrowProps.PreBuffer = false;
        using var reader = new FileReader(filePath, arrowProperties: arrowProps);
        using var batchReader = reader.GetRecordBatchReader();
        var batchCount = 0;
        while (await batchReader.ReadNextRecordBatchAsync() is { } batch)
        {
            batchCount++;
            batch.Dispose();
        }

        Console.WriteLine($"Read {batchCount} batches");
    }

    static Task ReadWithPqs(string filePath)
    {
        using var reader = new ParquetFileReader(filePath);
        var buffer = new float[1024 * 1024];
        for (int rg = 0; rg < reader.FileMetaData.NumRowGroups; rg++)
        {
            using var rgReader = reader.RowGroup(rg);
            for (int col = 2; col < rgReader.MetaData.NumColumns; col++)
            {
                var rowsRead = 0;
                using var columnReader = rgReader.Column(col).LogicalReader<float>();
                while (rowsRead < rgReader.MetaData.NumRows)
                {
                    rowsRead += columnReader.ReadBatch(buffer);
                }
            }
        }

        return Task.CompletedTask;
    }
}
