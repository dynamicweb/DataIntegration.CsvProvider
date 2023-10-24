using CsvHelper;
using CsvHelper.Configuration;
using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace Dynamicweb.DataIntegration.Providers.CsvProvider;

public class CsvSourceReader : ISourceReader
{
    private readonly Mapping mapping;

    public virtual int ColumnCount
    {
        get { throw new NotImplementedException(); }
    }

    private readonly string path;
    private readonly bool firstRowContainsColumnNames;
    private readonly char delimiter;
    private readonly char _quote;
    private readonly bool _ignoreDefectiveRows;
    private readonly string decimalSeparator;
    private readonly bool autoDetectDecimalSeparator;
    private readonly Encoding encoding;
    private StreamReader textReader;
    private CsvReader reader;
    private readonly CsvProvider _provider;
    private static bool _rowBadDataFound = false;

    private CsvReader Reader
    {
        get
        {
            if (reader == null)
            {
                var currentEncoding = encoding ?? Encoding.UTF8;
                textReader = new StreamReader(path, currentEncoding);
                var config = new CsvConfiguration(CultureInfo.CurrentCulture)
                {
                    Delimiter = delimiter + "",
                    Encoding = currentEncoding,
                    HasHeaderRecord = firstRowContainsColumnNames,
                    TrimOptions = TrimOptions.Trim,
                    Comment = _quote,
                    Escape = _quote,
                    Quote = _quote,
                };                    
                if (_ignoreDefectiveRows)
                {
                    config.BadDataFound = OnBadDataFound;
                }
                reader = new CsvReader(textReader, config); //firstRowContainsColumnNames, delimiter, quote, quote, '¤', ValueTrimmingOptions.All
            }
            return reader;
        }

    }

    private void OnBadDataFound(BadDataFoundArgs args)
    {
        logger.Log(string.Format("Skip failed row. Failed Field: {0}. Failed row: {1}.", args.Field, args.RawRecord));
        _rowBadDataFound = true;
    }

    private readonly ILogger logger;

    internal CsvSourceReader(CsvReader reader, Mapping mapping, bool firstRowContainsColumnNames, char delimiter, char quote)
    {
        this.firstRowContainsColumnNames = firstRowContainsColumnNames;
        this.reader = reader;
        this.mapping = mapping;
        this.delimiter = delimiter;
        this._quote = quote;
        VerifyDuplicateColumns();
    }

    public CsvSourceReader(string filePath, Mapping mapping, bool firstRowContainsColumnNames, char delimiter, char quote, Encoding encoding,
        string decimalSeparator, bool autoDetectDecimalSeparator, bool ignoreDefectiveRows, ILogger logger, CsvProvider provider)
    {
        this.firstRowContainsColumnNames = firstRowContainsColumnNames;
        path = filePath;
        this.mapping = mapping;
        this.delimiter = delimiter;
        this._quote = quote;
        this.encoding = encoding;
        this.decimalSeparator = decimalSeparator;
        this.autoDetectDecimalSeparator = autoDetectDecimalSeparator;
        this._ignoreDefectiveRows = ignoreDefectiveRows;
        VerifyDuplicateColumns();
        this.logger = logger;
        _provider = provider;
    }

    public CsvSourceReader()
    {
    }

    public bool IsDone()
    {
        if (Reader.Read())
        {
            Dictionary<string, object> result = ReadNextRecord();

            if (result != null && !result.Any())
            {
                bool equal = true;
                if (nextResult != null)
                {
                    foreach (KeyValuePair<string, object> keyValuePair in result)
                    {
                        if (result[keyValuePair.Key].ToString() != nextResult[keyValuePair.Key].ToString())
                            equal = false;
                    }

                    if (equal)
                    {
                        return true;
                    }
                }
                else
                {
                    return true;
                }
            }

            nextResult = result;

            if (result != null && RowMatchesConditions())
            {
                return false;
            }

            return IsDone();
        }
        else
        {
            return true;
        }
    }

    private bool RowMatchesConditions()
    {
        foreach (MappingConditional conditional in mapping.Conditionals)
        {
            if (!_provider.CheckCondition(conditional, nextResult))
            {
                return false;
            }
        }
        return true;
    }

    Dictionary<string, object> nextResult;

    public Dictionary<string, object> GetNext()
    {
        return nextResult;
    }

    private Dictionary<string, object> ReadNextRecord()
    {
        _rowBadDataFound = false;
        Dictionary<string, object> result = new Dictionary<string, object>();
        foreach (ColumnMapping cm in mapping.GetColumnMappings())
        {
            if (cm.Active && cm.SourceColumn != null && !result.ContainsKey(cm.SourceColumn.Name))
            {
                try
                {
                    KeyValuePair<string, object> kvp = GetValuesFromReader(cm);
                    if(_ignoreDefectiveRows && _rowBadDataFound)
                    {
                        if (Reader.Read())
                        {
                            return ReadNextRecord();
                        }
                        else
                        {
                            result = null;
                            break;
                        }
                    }
                    result.Add(kvp.Key, kvp.Value);
                }
                catch (Exception ex)
                {
                    string lineData = Reader?.Parser?.RawRecord;
                    int line = Reader?.Parser?.Row ?? 0;
                    line = (line > 0 && firstRowContainsColumnNames) ? line - 1 : line;

                    if (_ignoreDefectiveRows)
                    {
                        logger.Log(string.Format("Skip failed row: {0}", lineData));
                        if (Reader.Read())
                        {
                            return ReadNextRecord();
                        }
                        else
                        {
                            result = null;
                            break;
                        }
                    }
                    else
                    {
                        if (ex is CsvHelper.MissingFieldException)
                        {
                            throw new Exception(string.Format("MissingField Error in the file: {0}, line: {1}, line row: {2}.", path, line, lineData));
                        }
                        else
                        {
                            throw new Exception(string.Format("Error in the file: {0}, line: {1}, line row: {2}.", path, line, lineData), ex);
                        }
                    }
                }
            }
        }
        return result;
    }

    private KeyValuePair<string, object> GetValuesFromReader(ColumnMapping cm)
    {
        KeyValuePair<string, object> result = new KeyValuePair<string, object>();
        if (Reader[mapping.SourceTable.Columns.IndexOf(cm.SourceColumn)] == "NULL")
        {
            result = new KeyValuePair<string, object>(cm.SourceColumn.Name, DBNull.Value);
        }
        else
        {
            string value = Reader[mapping.SourceTable.Columns.IndexOf(cm.SourceColumn)];
            if (!string.IsNullOrEmpty(value) && cm.DestinationColumn != null &&
                (cm.DestinationColumn.Type == typeof(double) || cm.DestinationColumn.Type == typeof(float)))
            {
                if (autoDetectDecimalSeparator)
                {
                    value = Converter.ToDouble(value).ToString();
                }
                else if (!string.IsNullOrEmpty(decimalSeparator))
                {
                    value = value.Replace(decimalSeparator, CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);
                }
            }
            result = new KeyValuePair<string, object>(cm.SourceColumn.Name, value);
        }
        return result;
    }

    private void VerifyDuplicateColumns()
    {
        if (Reader != null && firstRowContainsColumnNames)
        {
            Reader.Read();
            Reader.ReadHeader();
            string[] headers = Reader.HeaderRecord;
            List<string> repeatedHeaders = new List<string>();
            List<string> seenHeaders = new List<string>();
            foreach (string header in headers)
            {
                if (seenHeaders.Contains(header))
                {
                    if (!repeatedHeaders.Contains(header) && !string.IsNullOrEmpty(header))
                        repeatedHeaders.Add(header);
                }
                else
                {
                    seenHeaders.Add(header);
                }
            }
            if (repeatedHeaders.Count > 0)
            {
                throw new Exception(string.Format("File {0}.csv : repeated columns found: {1}. " +
                    "If there are no column names in the csv file please uncheck 'First row in source files contains column names' option in the source settings.",
                    mapping.SourceTable.Name, string.Join(",", repeatedHeaders.ToArray())));
            }
        }
    }

    #region IDisposable Implementation

    protected bool Disposed;

    protected virtual void Dispose(bool disposing)
    {
        reader.Dispose();
        lock (this)
        {
            // Do nothing if the object has already been disposed of.
            if (Disposed)
                return;

            if (disposing)
            {
                // Release diposable objects used by this instance here.

                if (textReader != null)
                    textReader.Close();
                if (reader != null)
                    reader.Dispose();
            }

            // Release unmanaged resources here. Don't access reference type fields.

            // Remember that the object has been disposed of.
            Disposed = true;
        }
    }

    public virtual void Dispose()
    {
        Dispose(true);
        // Unregister object for finalization.
        GC.SuppressFinalize(this);
    }

    #endregion
}
