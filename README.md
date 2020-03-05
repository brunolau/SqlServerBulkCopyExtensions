# SqlServerBulkCopyExtensions
This class extends the standard behavior of SqlBulkCopy class allowing automatic retrieval of the indetity value of inserted data

## Feeds

* NuGet [![NuGet](https://img.shields.io/nuget/vpre/SqlServerBulkCopyExtensions.svg)](https://www.nuget.org/profiles/SqlServerBulkCopyExtensions)

## Let's get started

From **NuGet**:
* `Install-Package SqlServerBulkCopyExtensions` - .NET Core

## Sample usage

```cs
using SqlServerBulkCopyExtensions;
using System;
using System.Data;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    class Program
    {
        private class MyDataEntity
        {
            public int Id { get; set; }
            public int Quantity { get; set; }
            public string Name { get; set; }
        }

        static void Main(string[] args)
        {
            MainAsync().Wait();
        }

        static async Task MainAsync()
        {
            var dt = new DataTable(nameof(MyDataEntity));
            dt.Columns.Add(nameof(MyDataEntity.Id), typeof(int));
            dt.Columns.Add(nameof(MyDataEntity.Quantity), typeof(int));
            dt.Columns.Add(nameof(MyDataEntity.Name), typeof(string));

            for (int i = 0; i < 100; i++)
            {
                var newRow = dt.NewRow();
                newRow[nameof(MyDataEntity.Quantity)] = i;
                newRow[nameof(MyDataEntity.Name)] = "Name " + i;
                dt.Rows.Add(newRow);
            }


            using (var sqlBulk = new SqlBulkCopyExtended("YOUR CONN STRING"))
            {
                sqlBulk.IdentityColumnName = nameof(MyDataEntity.Id);
                sqlBulk.DestinationTableName = nameof(MyDataEntity);
                await sqlBulk.WriteToServerAsync(dt);
            }

            foreach (DataRow row in dt.Rows)
            {
                Console.WriteLine("Added identity value for row with name " + row["Name"].ToString() + " is " + row["Id"].ToString());
            }
        }
    }
}
```