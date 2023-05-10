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

namespace Dynamicweb.DataIntegration.Providers.CsvProvider
{
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
        private readonly char quote;
        private readonly bool ignoreDefectiveRows;
        private readonly string decimalSeparator;
        private readonly bool autoDetectDecimalSeparator;
        private readonly Encoding encoding;
        private StreamReader textReader;
        private CsvReader reader;

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
                        Comment = quote,
                        Delimiter = delimiter + "",
                        Encoding = currentEncoding,
                        HasHeaderRecord = firstRowContainsColumnNames,
                        Escape = quote,
                        Quote = quote,
                        TrimOptions = TrimOptions.Trim
                    };
                    reader = new CsvReader(textReader, config); //firstRowContainsColumnNames, delimiter, quote, quote, '¤', ValueTrimmingOptions.All
                }
                return reader;
            }

        }

        private readonly ILogger logger;

        internal CsvSourceReader(CsvReader reader, Mapping mapping, bool firstRowContainsColumnNames, char delimiter, char quote)
        {
            this.firstRowContainsColumnNames = firstRowContainsColumnNames;
            this.reader = reader;
            this.mapping = mapping;
            this.delimiter = delimiter;
            this.quote = quote;
            VerifyDuplicateColumns();
        }

        public CsvSourceReader(string filePath, Mapping mapping, bool firstRowContainsColumnNames, char delimiter, char quote, Encoding encoding,
            string decimalSeparator, bool autoDetectDecimalSeparator, bool ignoreDefectiveRows, ILogger logger)
        {
            this.firstRowContainsColumnNames = firstRowContainsColumnNames;
            path = filePath;
            this.mapping = mapping;
            this.delimiter = delimiter;
            this.quote = quote;
            this.encoding = encoding;
            this.decimalSeparator = decimalSeparator;
            this.autoDetectDecimalSeparator = autoDetectDecimalSeparator;
            this.ignoreDefectiveRows = ignoreDefectiveRows;
            VerifyDuplicateColumns();
            this.logger = logger;
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

                if (RowMatchesConditions())
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
                var sourceColumnConditional = nextResult[conditional.SourceColumn.Name]?.ToString() ?? string.Empty;
                var theCondtion = conditional?.Condition ?? string.Empty;
                switch (conditional.ConditionalOperator)
                {
                    case ConditionalOperator.EqualTo:
                        if (!sourceColumnConditional.Equals(theCondtion))
                        {
                            return false;
                        }
                        break;
                    case ConditionalOperator.DifferentFrom:
                        if (sourceColumnConditional.Equals(theCondtion))
                        {
                            return false;
                        }
                        break;
                    case ConditionalOperator.Contains:
                        if (!sourceColumnConditional.Contains(theCondtion))
                        {
                            return false;
                        }
                        break;
                    case ConditionalOperator.LessThan:
                        string lessThanDecimalValue = theCondtion;
                        if (!string.IsNullOrEmpty(decimalSeparator) && decimalSeparator != System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator)
                        {
                            lessThanDecimalValue = lessThanDecimalValue.Replace(System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator, "");
                            lessThanDecimalValue = lessThanDecimalValue.Replace(decimalSeparator, System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);
                        }
                        if (Converter.ToDouble(sourceColumnConditional) >= Converter.ToDouble(lessThanDecimalValue))
                        {
                            return false;
                        }
                        break;
                    case ConditionalOperator.GreaterThan:
                        string greaterThanDecimalValue = theCondtion;
                        if (!string.IsNullOrEmpty(decimalSeparator) && decimalSeparator != System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator)
                        {
                            greaterThanDecimalValue = greaterThanDecimalValue.Replace(System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator, "");
                            greaterThanDecimalValue = greaterThanDecimalValue.Replace(decimalSeparator, System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);
                        }
                        if (Converter.ToDouble(sourceColumnConditional) <= Converter.ToDouble(greaterThanDecimalValue))
                        {
                            return false;
                        }
                        break;
                    case ConditionalOperator.In:
                        var inConditionalValue = theCondtion;
                        if (!string.IsNullOrEmpty(inConditionalValue))
                        {
                            List<string> inConditions = inConditionalValue.Split(',').Select(obj => obj.Trim()).ToList();
                            if (!inConditions.Contains(sourceColumnConditional))
                            {
                                return false;
                            }
                        }
                        break;
                    case ConditionalOperator.StartsWith:
                        if (!sourceColumnConditional.StartsWith(theCondtion))
                        {
                            return false;
                        }
                        break;
                    case ConditionalOperator.NotStartsWith:
                        if (sourceColumnConditional.StartsWith(theCondtion))
                        {
                            return false;
                        }
                        break;
                    case ConditionalOperator.EndsWith:
                        if (!sourceColumnConditional.EndsWith(theCondtion))
                        {
                            return false;
                        }
                        break;
                    case ConditionalOperator.NotEndsWith:
                        if (sourceColumnConditional.EndsWith(theCondtion))
                        {
                            return false;
                        }
                        break;
                    case ConditionalOperator.NotContains:
                        if (sourceColumnConditional.Contains(theCondtion))
                        {
                            return false;
                        }
                        break;
                    case ConditionalOperator.NotIn:
                        var notInConditionalValue = theCondtion;
                        if (!string.IsNullOrEmpty(notInConditionalValue))
                        {
                            List<string> notInConditions = notInConditionalValue.Split(',').Select(obj => obj.Trim()).ToList();
                            if (notInConditions.Contains(sourceColumnConditional))
                            {
                                return false;
                            }
                        }
                        break;
                    default:
                        break;
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
            Dictionary<string, object> result = new Dictionary<string, object>();
            foreach (ColumnMapping cm in mapping.GetColumnMappings())
            {
                if (cm.Active && cm.SourceColumn != null && !result.ContainsKey(cm.SourceColumn.Name))
                {
                    try
                    {
                        KeyValuePair<string, object> kvp = GetValuesFromReader(cm);
                        result.Add(kvp.Key, kvp.Value);
                    }
                    catch (Exception ex)
                    {
                        long line = firstRowContainsColumnNames ? Reader.CurrentIndex + 1 : Reader.CurrentIndex;
                        string lineData = GetErrorLine(line);
                        if (ignoreDefectiveRows)
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
                                throw new Exception(string.Format("Error in the file: {0}, line: {1}, line row: {2}.", path, line, lineData));
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

        private string GetErrorLine(long line)
        {
            using (var sr = new StreamReader(path))
            {
                for (long i = 0; i < line; i++)
                    sr.ReadLine();
                return sr.ReadLine();
            }
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
}
