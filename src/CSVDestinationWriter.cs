﻿using Dynamicweb.Core;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.ProviderHelpers;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace Dynamicweb.DataIntegration.Providers.CsvProvider
{
    public class CsvDestinationWriter : IDestinationWriter, IDisposable
    {
        private TextWriter writer;
        protected internal TextWriter Writer
        {
            get
            {
                if (writer == null)
                {
                    string fileName = Mapping.DestinationTable.Name;
                    if (includeTimestampInFileName)
                    {
                        fileName += DateTime.Now.ToString("yyyyMMdd-HHmmssFFFFFFF");
                    }
                    writer = new StreamWriter(path.CombinePaths(fileName + ".csv"), false, encoding);
                }
                return writer;
            }
            set { writer = value; }
        }


        public CsvDestinationWriter(TextWriter writer, Mapping map, bool firstRowContainsColumnNames, char seperator, char stringDelimiter, CultureInfo cultureInfo, bool includeTimestampInFileName)
        {
            Writer = writer;
            mapping = map;
            this.firstRowContainsColumnNames = firstRowContainsColumnNames;
            fieldDelimiter = seperator;
            quoteChar = stringDelimiter;
            this.cultureInfo = cultureInfo;
            this.includeTimestampInFileName = includeTimestampInFileName;
        }
        public CsvDestinationWriter()
        {
        }

        public CsvDestinationWriter(string path, Mapping mapping, bool firstRowContainsColumnNames, char fieldDelimiter, char quoteChar, Encoding encoding, CultureInfo cultureInfo, bool includeTimestampInFileName)
        {
            this.mapping = mapping;
            this.path = path;
            this.firstRowContainsColumnNames = firstRowContainsColumnNames;
            this.fieldDelimiter = fieldDelimiter;
            this.quoteChar = quoteChar;
            this.encoding = encoding;
            this.cultureInfo = cultureInfo;
            this.includeTimestampInFileName = includeTimestampInFileName;

            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);
        }

        private readonly Mapping mapping;
        private readonly bool firstRowContainsColumnNames;
        private readonly char fieldDelimiter;
        private readonly char quoteChar;
        private bool initialized;
        private readonly string path;
        private readonly Encoding encoding = Encoding.UTF8;
        private readonly CultureInfo cultureInfo;
        private readonly bool includeTimestampInFileName;

        public virtual Mapping Mapping
        {
            get { return mapping; }
        }

        public virtual void Write(Dictionary<string, object> row)
        {

            if (!mapping.Conditionals.CheckConditionals(row))
                return;

            if (!initialized && firstRowContainsColumnNames)
            {
                InitializeFile();
            }
            string stringToWrite = Mapping.GetColumnMappings().Where(columnMapping => columnMapping.Active).Aggregate("", (current, columnMapping) => current + GetStringToWrite(row, columnMapping));
            if (stringToWrite != "")
                stringToWrite = stringToWrite.Substring(0, stringToWrite.Length - 1);
            stringToWrite = stringToWrite.Replace(System.Environment.NewLine, "");
            Writer.WriteLine(stringToWrite);
        }

        private string GetStringToWrite(Dictionary<string, object> row, ColumnMapping columnMapping)
        {
            string stringToWrite;
            if (columnMapping.SourceColumn == null && columnMapping.HasScriptWithValue)
            {
                stringToWrite = quoteChar + columnMapping.GetScriptValue() + quoteChar + fieldDelimiter;
            }
            else if (row.ContainsKey(columnMapping.SourceColumn.Name))
            {
                if (row[columnMapping.SourceColumn.Name] == DBNull.Value)
                {
                    if (columnMapping.HasScriptWithValue)
                    {
                        stringToWrite = quoteChar + columnMapping.GetScriptValue() + quoteChar + fieldDelimiter;
                    }
                    else
                    {
                        stringToWrite = "NULL" + fieldDelimiter;
                    }
                }
                else
                {
                    if (columnMapping.SourceColumn.Type == typeof(string))
                    {
                        stringToWrite = row[columnMapping.SourceColumn.Name].ToString().Replace(quoteChar.ToString(CultureInfo.CurrentCulture),
                                            quoteChar.ToString(CultureInfo.CurrentCulture) + quoteChar.ToString(CultureInfo.CurrentCulture));
                    }
                    else if (columnMapping.SourceColumn.Type == typeof(DateTime))
                    {
                        stringToWrite = ((DateTime)row[columnMapping.SourceColumn.Name]).ToString(
                                            "dd-MM-yyyy HH:mm:ss:fff", CultureInfo.CurrentCulture);
                    }
                    else if (cultureInfo != null && (columnMapping.SourceColumn.Type == typeof(int) ||
                                columnMapping.SourceColumn.Type == typeof(decimal) ||
                                columnMapping.SourceColumn.Type == typeof(double) ||
                                columnMapping.SourceColumn.Type == typeof(float)))
                    {
                        stringToWrite = ValueFormatter.GetFormattedValue(row[columnMapping.SourceColumn.Name], cultureInfo, columnMapping.ScriptType, columnMapping.ScriptValue);
                    }
                    else
                    {
                        stringToWrite = row[columnMapping.SourceColumn.Name].ToString();
                    }
                    switch (columnMapping.ScriptType)
                    {
                        case ScriptType.Append:
                            stringToWrite = quoteChar + stringToWrite + columnMapping.ScriptValue + quoteChar + fieldDelimiter;
                            break;
                        case ScriptType.Constant:
                            stringToWrite = quoteChar + columnMapping.ScriptValue + quoteChar + fieldDelimiter;
                            break;
                        case ScriptType.Prepend:
                            stringToWrite = quoteChar + columnMapping.ScriptValue + stringToWrite + quoteChar + fieldDelimiter;
                            break;
                        case ScriptType.None:
                            stringToWrite = quoteChar + stringToWrite + quoteChar + fieldDelimiter;
                            break;
                        case ScriptType.NewGuid:
                            stringToWrite = quoteChar + columnMapping.GetScriptValue() + quoteChar + fieldDelimiter;
                            break;
                    }
                }
            }
            else
            {
                throw new Exception(BaseDestinationWriter.GetRowValueNotFoundMessage(row, columnMapping.SourceColumn.Table.Name, columnMapping.SourceColumn.Name));
            }
            return stringToWrite;
        }


        private void InitializeFile()
        {
            string columnNames = Mapping.GetColumnMappings().Where(columnMapping => columnMapping.Active).Aggregate("", (current, columnMapping) => current + (quoteChar + GetColumnName(columnMapping) + quoteChar + fieldDelimiter));
            columnNames = columnNames.Substring(0, columnNames.Length - 1);
            Writer.WriteLine(columnNames);
            initialized = true;
        }

        private string GetColumnName(ColumnMapping columnMapping)
        {
            if (columnMapping.ScriptType == ScriptType.Constant)
            {
                return columnMapping.DestinationColumn?.Name;
            }
            return columnMapping.DestinationColumn.Name;
        }

        public virtual void Close()
        {
            Writer.Close();
        }

        #region IDisposable Implementation

        protected bool Disposed;

        protected virtual void Dispose(bool disposing)
        {
            lock (this)
            {
                // Do nothing if the object has already been disposed of.
                if (Disposed)
                    return;

                if (disposing)
                {
                    // Release diposable objects used by this instance here.

                    if (writer != null)
                        writer.Dispose();
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
