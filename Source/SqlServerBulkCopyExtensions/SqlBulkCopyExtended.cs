using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;

namespace SqlServerBulkCopyExtensions
{
    /// <summary>
    /// Extension to the <see cref="Microsoft.Data.SqlClient.SqlBulkCopy"/> class allowing automatic Identity column retrieval
    /// </summary>
    public class SqlBulkCopyExtended : IDisposable
    {
        private SqlConnection _connection;
        private List<SqlBulkCopyColumnMapping> _columnMappings;
        private readonly bool _disposeConnection;
        private readonly SqlBulkCopyOptions _copyOptions;

        /// <summary>
        /// Initializes a new instance using the specified open instance of Microsoft.Data.SqlClient.SqlConnection .
        /// </summary>
        /// <param name="connection">
        /// The already open <see cref="Microsoft.Data.SqlClient.SqlConnection"/> instance that will be
        /// used to perform the bulk copy operation. If your connection string does not use
        /// Integrated Security = true , you can use <see cref="Microsoft.Data.SqlClient.SqlCredential"/>
        /// to pass the user ID and password more securely than by specifying the user ID
        /// and password as text in the connection string.
        /// </param>
        public SqlBulkCopyExtended(SqlConnection connection)
        {
            _connection = connection;
        }

        /// <summary>
        /// Initializes a new instance using the specified connection string, new connection will be created internally
        /// </summary>
        /// <param name="connectionString">
        /// The database connection string
        /// </param>
        public SqlBulkCopyExtended(string connectionString) : this(new SqlConnection(connectionString))
        {
            _disposeConnection = true;
        }

        /// <summary>
        /// Initializes a new instance using the specified connection string, new connection will be created internally
        /// </summary>
        /// <param name="connectionString">
        /// The database connection string
        /// </param>
        /// <param name="copyOptions">
        /// A combination of values from the <see cref="Microsoft.Data.SqlClient.SqlBulkCopyOptions"></see> 
        /// enumeration that determines which data source rows are copied to the destination table.
        /// </param>
        public SqlBulkCopyExtended(string connectionString, SqlBulkCopyOptions copyOptions) : this(new SqlConnection(connectionString), copyOptions)
        {
            _disposeConnection = true;
        }

        /// <summary>
        /// Initializes a new instance using the specified connection string, new connection will be created internally
        /// </summary>
        /// <param name="connection">
        /// The already open <see cref="Microsoft.Data.SqlClient.SqlConnection"/> instance that will be
        /// used to perform the bulk copy operation. If your connection string does not use
        /// Integrated Security = true , you can use <see cref="Microsoft.Data.SqlClient.SqlCredential"/>
        /// to pass the user ID and password more securely than by specifying the user ID
        /// and password as text in the connection string.
        /// </param>
        /// <param name="copyOptions">
        /// A combination of values from the <see cref="Microsoft.Data.SqlClient.SqlBulkCopyOptions"></see> 
        /// enumeration that determines which data source rows are copied to the destination table.
        /// </param>
        public SqlBulkCopyExtended(SqlConnection connection, SqlBulkCopyOptions copyOptions)
        {
            _connection = connection;
            _copyOptions = copyOptions;
        }

        /// <summary>
        /// Name of the destination table on the server.
        /// </summary>
        public string DestinationTableName { get; set; }

        /// <summary>
        /// Enables or disables the bulk copy object to stream data from an <see cref="System.Data.IDataReader"/> object
        /// </summary>
        public bool EnableStreaming { get; set; }

        /// <summary>
        /// Number of seconds for the operation to complete before it times out.
        /// </summary>
        public int BulkCopyTimeoutSeconds { get; set; }

        /// <summary>
        /// Number of rows in each batch. At the end of each batch, the rows in the batch
        /// are sent to the server.
        /// </summary>
        public int BatchSize { get; set; }

        /// <summary>
        /// Defines the number of rows to be processed before generating a notification event.
        /// </summary>
        public int NotifyAfter { get; set; }

        /// <summary>
        /// Name of the identity column in the database. 
        /// Has to be spcified so that the engine knows which column should be treated as the identity
        /// </summary>
        public string IdentityColumnName { get; set; } 

        /// <summary>
        /// Returns a collection of <see cref="Microsoft.Data.SqlClient.SqlBulkCopyColumnMapping"/> items.
        /// Column mappings define the relationships between columns in the data source and
        /// columns in the destination.
        /// </summary>
        public List<SqlBulkCopyColumnMapping> ColumnMappings
        {
            get
            {
                if (_columnMappings == null)
                    _columnMappings = new List<SqlBulkCopyColumnMapping>();

                return _columnMappings;
            }
        }

        /// <summary>
        /// Copies all rows in the supplied System.Data.DataTable to a destination table
        /// specified by the <see cref="SqlServerBulkCopyExtensions.SqlBulkCopyExtended.DestinationTableName"/> property
        /// and retrieves inserted identites based on the <see cref="SqlServerBulkCopyExtensions.SqlBulkCopyExtended.IdentityColumnName"/> property settings
        /// </summary>
        /// <param name="table">
        /// A <see cref="System.Data.DataTable"/> whose rows will be copied to the destination table.
        /// </param>
        /// <returns></returns>
        public async Task WriteToServerAsync(DataTable table)
        {
            if (string.IsNullOrEmpty(IdentityColumnName))
                throw new ArgumentException("IdentityColumnName is not specified", nameof(IdentityColumnName));

            var connectionWasNotOpen = _connection.State != ConnectionState.Open;
            var tempTableBulk = GetTempTableName(table, "B");
            var templTableRetrieval = GetTempTableName(table, "R");
            var tablesDropped = false;

            try
            {
                //Open the connection
                if (_connection.State != ConnectionState.Open)
                {
                    await _connection.OpenAsync();
                }

                //Create temporary tables
                await GetCreateTempTablesCommand(tempTableBulk, templTableRetrieval).ExecuteNonQueryAsync();

                //Perform bulk insert
                using (var sqlBulkCopy = GetSqlBulkCopy())
                {
                    sqlBulkCopy.DestinationTableName = tempTableBulk;
                    await sqlBulkCopy.WriteToServerAsync(table);
                }

                //Retrieve identity values and drop tables
                var readerIndex = 0;
                using (var reader = await GetReadSqlCommand(table, tempTableBulk, templTableRetrieval).ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        table.Rows[readerIndex][IdentityColumnName] = reader.GetValue(0);
                        readerIndex += 1;
                    }
                }

                //Indicate tables were dropped
                tablesDropped = true;
            }
            finally
            {
                //If not dropped, drop them
                if (!tablesDropped)
                {
                    var cmd = _connection.CreateCommand();
                    cmd.CommandText = "DROP TABLE " + tempTableBulk + " DROP TABLE " + templTableRetrieval;
                    await cmd.ExecuteNonQueryAsync();
                }

                //Close connection if it was not open prior
                if (connectionWasNotOpen)
                {
                    await _connection.CloseAsync();
                }
            }
        }

        private SqlCommand GetCreateTempTablesCommand(string bulkTable, string retrievalTable)
        {
            var cmd = _connection.CreateCommand();
            cmd.CommandText =
                @"DECLARE @RetrievalScript NVARCHAR(1000) =  CONCAT('ALTER TABLE " + retrievalTable + " ALTER Column " + IdentityColumnName + @" ', (SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + DestinationTableName + "' AND COLUMN_NAME = '" + IdentityColumnName + @"'))
                  SELECT * INTO " + bulkTable + " FROM " + DestinationTableName + @" WHERE 1 = 0
                  CREATE TABLE " + retrievalTable + " (" + IdentityColumnName + @" int)
                  EXEC(@RetrievalScript)";

            return cmd;
        }

        private SqlCommand GetReadSqlCommand(DataTable dt, string bulkTable, string retrievalTable)
        {
            var columns = GetColumnList(dt);
            var readCommand = _connection.CreateCommand();
            readCommand.CommandText =
                @"INSERT INTO " + DestinationTableName + "(" + columns + @") OUTPUT INSERTED." + IdentityColumnName + " INTO " + retrievalTable + " SELECT " + columns + " FROM " + bulkTable + " SELECT * FROM " + retrievalTable + " ORDER BY " + IdentityColumnName + " DROP TABLE " + bulkTable + " DROP TABLE " + retrievalTable;

            return readCommand;

        }

        private string GetColumnList(DataTable dt)
        {
            var columnBuilder = new StringBuilder();
            foreach (DataColumn dtCol in dt.Columns)
            {
                var columnName = dtCol.ColumnName;
                var colMapping = GetCopyColumnMapping(dtCol);
                if (colMapping != null)
                {
                    columnName = colMapping.DestinationColumn;
                }

                if (columnName != IdentityColumnName)
                {
                    columnBuilder.Append(columnName + ",");
                }
            }
            columnBuilder.Remove(columnBuilder.Length - 1, 1);

            return columnBuilder.ToString();
        }

        private string GetTempTableName(DataTable dt, string suffix)
        {
            return "#" + dt.TableName + "T" + suffix;
        }



        private SqlBulkCopyColumnMapping GetCopyColumnMapping(DataColumn col)
        {
            if (_columnMappings == null || _columnMappings.Count == 0)
            {
                return null;
            }

            foreach (SqlBulkCopyColumnMapping colMap in ColumnMappings)
            {
                if (colMap.SourceColumn == col.ColumnName)
                {
                    return colMap;
                }
            }

            return null;
        }

        private SqlBulkCopy GetSqlBulkCopy()
        {
            var retVal = new SqlBulkCopy(_connection, _copyOptions, null)
            {
                BatchSize = BatchSize,
                BulkCopyTimeout = BulkCopyTimeoutSeconds,
                DestinationTableName = DestinationTableName,
                EnableStreaming = EnableStreaming,
                NotifyAfter = NotifyAfter
            };

            if (_columnMappings != null)
            {
                foreach (SqlBulkCopyColumnMapping mapping in _columnMappings)
                {
                    retVal.ColumnMappings.Add(mapping);
                }
            }

            return retVal;
        }

        /// <summary>
        /// Disposes the <see cref="SqlServerBulkCopyExtensions.SqlBulkCopyExtended"/> instance
        /// </summary>
        public void Dispose()
        {
            if (_disposeConnection)
            {
                _connection.Dispose();
            }

            _connection = null;
            _columnMappings = null;
        }
    }
}
