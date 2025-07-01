using ParquetSharp;
using ParquetSharp.Arrow;
using ParquetSharp.Dataset;

namespace Benchmark;

class Program
{
    static async Task Main(string[] args)
    {
        await ReadDataset(args[0], dateFilter: true, idFilter: true);
        //await ReadWithArrow($"{args[0]}/test_data.parquet");
        //await ReadWithPqs($"{args[0]}/test_data.parquet");
    }

    static async Task ReadDataset(string datasetPath, bool dateFilter = false, bool idFilter = false)
    {
        using var arrowProperties = ArrowReaderProperties.GetDefault();
        arrowProperties.PreBuffer = false;
        using var readerProperties = ReaderProperties.GetDefaultReaderProperties();
        readerProperties.EnableBufferedStream();
        
        var footerKey = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        using var fileDecryptionProperties = new FileDecryptionPropertiesBuilder().FooterKey(footerKey).Build();
        readerProperties.FileDecryptionProperties = fileDecryptionProperties;

        var reader = new DatasetReader(
            datasetPath, readerProperties: readerProperties, arrowReaderProperties: arrowProperties);
        IFilter? filter = null;
        if (dateFilter)
        {
            filter = Col.Named("date").IsInRange(new DateOnly(2025, 1, 2), new DateOnly(2025, 1, 4));
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
