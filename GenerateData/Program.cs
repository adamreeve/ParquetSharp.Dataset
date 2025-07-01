using ParquetSharp;

namespace GenerateData;

public class Program
{
    public static void Main(string[] args)
    {
        var columns = new List<Column>
        {
            new Column<DateOnly>("date"),
            new Column<int>("id"),
        };
        const int numFloatCols = 451;
        for (var i = 0; i < numFloatCols; ++i)
        {
            columns.Add(new Column<float>($"x{i}"));
        }

        using var writerProperties = new WriterPropertiesBuilder()
            .Compression(Compression.Snappy)
            .DisableDictionary()
            .Build();

        using var writer = new ParquetFileWriter(args[0], columns.ToArray(), writerProperties);

        const int totalRows = 6_624_387;
        const int rowsPerRowGroup = 1024*1024;
        var date = new DateOnly(2025, 1, 1);

        var dates = new DateOnly[rowsPerRowGroup];
        var ids = new int[rowsPerRowGroup];
        var x = new float[rowsPerRowGroup];
        var rng = new Random(123);

        var rowsWritten = 0;
        var rowGroupIndex = 0;
        while (rowsWritten < totalRows)
        {
            Console.WriteLine($"Row group {rowGroupIndex}");
            var rowsToWrite = Math.Min(totalRows - rowsWritten, rowsPerRowGroup);
            for (var i = 0; i < rowsToWrite; i++)
            {
                dates[i] = date;
                ids[i] = i % 100;
            }

            using var rowGroup = writer.AppendRowGroup();
            using var dateWriter = rowGroup.NextColumn().LogicalWriter<DateOnly>();
            dateWriter.WriteBatch(dates.AsSpan(0, rowsToWrite));
            using var idWriter = rowGroup.NextColumn().LogicalWriter<int>();
            idWriter.WriteBatch(ids.AsSpan(0, rowsToWrite));
            for (var colIdx = 0; colIdx < numFloatCols; colIdx++)
            {
                for (var i = 0; i < rowsToWrite; i++)
                {
                    x[i] = rng.NextSingle();
                }

                using var floatWriter = rowGroup.NextColumn().LogicalWriter<float>();
                floatWriter.WriteBatch(x.AsSpan(0, rowsToWrite));
            }

            date = date.AddDays(1);
            rowsWritten += rowsToWrite;
            rowGroupIndex += 1;
        }

        writer.Close();
    }
}
