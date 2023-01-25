﻿using CsvHelper;
using CsvHelper.Configuration;
using Dynamicweb.Core;
using Dynamicweb.Core.Helpers;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.Ecommerce.International;
using Dynamicweb.Extensibility.AddIns;
using Dynamicweb.Extensibility.Editors;
using Dynamicweb.Logging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Xml;
using System.Xml.Linq;

[assembly: InternalsVisibleTo("Dynamicweb.Tests")]
[assembly: InternalsVisibleTo("Dynamicweb.Test.Integration")]

namespace Dynamicweb.DataIntegration.Providers.CsvProvider
{
    [AddInName("Dynamicweb.DataIntegration.Providers.Provider"), AddInLabel("CSV Provider"), AddInDescription("CSV provider"), AddInIgnore(false)]
    public class CsvProvider : BaseProvider, ISource, IDestination, IDropDownOptions
    {
        [AddInParameter("First row in source files contains column names"),
         AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Source")]
        public virtual bool SourceFirstRowContainsColumnNames
        {
            get
            {
                return sourceFirstRowContainsColumnNames;
            }
            set
            {
                sourceFirstRowContainsColumnNames = value;
            }
        }
        private bool sourceFirstRowContainsColumnNames = true;

        [AddInParameter("First row in destination files shall contain column names"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Destination")]
        public virtual bool DestinationFirstRowContainsColumnNames
        {
            get
            {
                return destinationFirstRowContainsColumnNames;
            }
            set
            {
                destinationFirstRowContainsColumnNames = value;
            }
        }
        private bool destinationFirstRowContainsColumnNames = true;

        [AddInParameter("Include timestamp in filename"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Destination")]
        public virtual bool IncludeTimestampInFileName
        {
            get;
            set;
        }

        //path should point to a folder - if it doesn't, write will fail.
        [AddInParameter("Source folder"), AddInParameterEditor(typeof(FolderSelectEditor), "htmlClass=NewUIinput;"), AddInParameterGroup("Source")]
        public string SourcePath
        {
            get { return _path; }
            set { _path = value; }
        }
        [AddInParameter("Source file"), AddInParameterEditor(typeof(FileManagerEditor), "inputClass=NewUIinput;folder=/Files/;Icon=fa fa-exclamation-triangle;Tooltip=Selecting a source file will override source folder selection;allowBrowse=true;"), AddInParameterGroup("Source")]
        public string SourceFile { get; set; }

        [AddInParameter("Delete source files"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Source")]
        public bool DeleteSourceFiles { get; set; }

        [AddInParameter("Destination folder"), AddInParameterEditor(typeof(FolderSelectEditor), "htmlClass=NewUIinput;"), AddInParameterGroup("Destination")]
        public string DestinationPath
        {
            get { return _path; }
            set { _path = value; }
        }
        private string _path = "";

        private Schema schema;
        private readonly Dictionary<string, CsvReader> csvReadersForTest;
        private List<CsvDestinationWriter> destinationWriters;

        [AddInParameter("Input Field delimiter"), AddInParameterEditor(typeof(TextParameterEditor), "maxLength=1;inputClass=NewUIinput;"), AddInParameterGroup("Source")]
        public char SourceFieldDelimiter
        {
            get { return fieldDelimiter; }
            set { fieldDelimiter = value; }
        }
        [AddInParameter("Output Field delimiter"), AddInParameterEditor(typeof(TextParameterEditor), "maxLength=1;inputClass=NewUIinput;"), AddInParameterGroup("Destination")]
        public char DestinationFieldDelimiter
        {
            get { return fieldDelimiter; }
            set { fieldDelimiter = value; }
        }
        private char fieldDelimiter = ';';

        [AddInParameter("Input string delimiter"), AddInParameterEditor(typeof(TextParameterEditor), "maxLength=1;inputClass=NewUIinput;"), AddInParameterGroup("Source")]
        public char SourceQuoteCharacter
        {
            get { return quoteChar; }
            set { quoteChar = value; }
        }
        [AddInParameter("Output string delimiter"), AddInParameterEditor(typeof(TextParameterEditor), "maxLength=1;inputClass=NewUIinput;"), AddInParameterGroup("Destination")]
        public char DestinationQuoteCharacter
        {
            get { return quoteChar; }
            set { quoteChar = value; }
        }
        [AddInParameter("Source encoding"), AddInParameterEditor(typeof(DropDownParameterEditor), "NewGUI=true;"), AddInParameterGroup("Source")]
        public string SourceEncoding { get; set; }

        [AddInParameter("Destination encoding"), AddInParameterEditor(typeof(DropDownParameterEditor), "NewGUI=true;"), AddInParameterGroup("Destination")]
        public string DestinationEncoding { get; set; }

        [AddInParameter("Number format culture"), AddInParameterEditor(typeof(DropDownParameterEditor), ""), AddInParameterGroup("Destination")]
        public string ExportCultureInfo { get; set; }

        private readonly string DetectAutomaticallySeparator = "Detect automatically";
        private readonly string NoneDecimalSeparator = "Use system culture";
        private string sourceDecimalSeparator;
        [AddInParameter("Source decimal separator"), AddInParameterEditor(typeof(DropDownParameterEditor), "NewGUI=true;none=false"), AddInParameterGroup("Source")]
        public string SourceDecimalSeparator
        {
            get
            {
                if (string.IsNullOrEmpty(sourceDecimalSeparator))
                {
                    return DetectAutomaticallySeparator;
                }
                else
                {
                    return sourceDecimalSeparator;
                }
            }
            set { sourceDecimalSeparator = value; }
        }

        [AddInParameter("Skip Troublesome rows"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Source")]
        public bool IgnoreDefectiveRows { get; set; }

        private string workingDirectory;
        public override string WorkingDirectory
        {
            get
            {
                return workingDirectory.CombinePaths(FilesFolderName);
            }
            set { workingDirectory = value; }
        }

        private char quoteChar = '"';

        public override bool SchemaIsEditable
        {
            get { return true; }
        }

        public override Schema GetOriginalSourceSchema()
        {
            Schema result = new Schema();

            if (csvReadersForTest != null && csvReadersForTest.Count > 0)//if unit tests are executing
            {
                GetSchemaForTableFromFile(result, csvReadersForTest);
            }
            else
            {
                Dictionary<string, CsvReader> csvReaders = new Dictionary<string, CsvReader>();
                var currentEncoding = string.IsNullOrEmpty(SourceEncoding) ? Encoding.UTF8 : GetEncoding(SourceEncoding);
                var config = new CsvConfiguration(CultureInfo.CurrentCulture)
                {
                    Comment = '¤',
                    Delimiter = fieldDelimiter + "",
                    Encoding = currentEncoding,
                    HasHeaderRecord = SourceFirstRowContainsColumnNames,
                    Escape = '\\',
                    Quote = quoteChar,
                    TrimOptions = TrimOptions.None,
                    DetectColumnCountChanges = true
                };

                foreach (string file in GetSourceFiles())
                {
                    var streamReader = new StreamReader(file, currentEncoding);
                    csvReaders.Add(Path.GetFileNameWithoutExtension(file), new CsvReader(streamReader, config));
                }

                try
                {
                    GetSchemaForTableFromFile(result, csvReaders);
                }
                finally
                {
                    //release readers to unlock csv files
                    foreach (var reader in csvReaders)
                        reader.Value.Dispose();
                }
            }
            return result;
        }

        public override Schema GetOriginalDestinationSchema()
        {
            return GetSchema();
        }

        public override void OverwriteSourceSchemaToOriginal()
        {
            schema = GetOriginalSourceSchema();
        }

        public override void OverwriteDestinationSchemaToOriginal()
        {
        }

        public override Schema GetSchema()
        {
            if (schema == null)
            {
                schema = GetOriginalSourceSchema();
            }
            return schema;
        }
        public override string ValidateDestinationSettings()
        {
            if (!Directory.Exists((workingDirectory + DestinationPath).Replace("\\", "/")))
                return "Destination folder " + DestinationPath + "does not exist";
            return "";
        }
        public override string ValidateSourceSettings()
        {
            if (string.IsNullOrEmpty(SourceFile) && string.IsNullOrEmpty(SourcePath))
            {
                return "No Source file neither folder are selected";
            }
            if (string.IsNullOrEmpty(SourceFile))
            {
                string srcFolderPath = (workingDirectory.CombinePaths(SourcePath)).Replace("\\", "/");

                if (!Directory.Exists(srcFolderPath))
                {
                    return "Source folder \"" + SourcePath + "\" does not exist";
                }
                else
                {
                    var files = Directory.GetFiles(srcFolderPath);

                    if (files.Length == 0)
                    {
                        return "There are no files in the source folder";
                    }
                }
            }
            else
            {
                if (!File.Exists(WorkingDirectory + SourceFile))
                {
                    return "Source file \"" + SourceFile + "\" does not exist";
                }
                else
                {
                    if (!SourceFile.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
                    {
                        return "Source file \"" + SourceFile + "\" is not csv file";
                    }
                }
            }
            if (!string.IsNullOrEmpty(SourceFile) && !string.IsNullOrEmpty(SourcePath))
            {
                return "Warning: In your CSV Provider source, you selected both a source file and a source folder. The source folder selection will be ignored, and only the source file will be used.";
            }
            return null;
        }

        private void GetSchemaForTableFromFile(Schema schema, Dictionary<string, CsvReader> csvReaders)
        {
            foreach (var reader in csvReaders)
            {
                Table csvTable = schema.AddTable(reader.Key);
                try
                {
                    var row = reader.Value.Read();
                    int columnCount = reader.Value.ColumnCount;
                    if (!SourceFirstRowContainsColumnNames)
                    {
                        for (int i = 1; i <= columnCount; i++)
                        {
                            csvTable.AddColumn(new Column("Column " + i, typeof(string), csvTable));
                        }
                    }
                    else
                    {
                        var header = reader.Value.ReadHeader();
                        for (int i = 0; i < columnCount; i++)
                        {
                            csvTable.AddColumn(new Column(reader.Value.HeaderRecord[i], typeof(string), csvTable));
                        }
                    }
                }
                catch (BadDataException csvEx)
                {
                    string msg = string.Format("Error reading CSV file: {0}.csv. Check field delimiter, it must be the same as in the provider settings: '{1}'.\nError message: {2}",
                        reader.Key, fieldDelimiter, csvEx.Message);
                    Logger?.Log(msg);
                    schema.RemoveTable(csvTable);
                }
            }
        }

        public new virtual void SaveAsXml(XmlTextWriter xmlTextWriter)
        {
            xmlTextWriter.WriteStartElement("SourceFieldDelimiter");
            xmlTextWriter.WriteCData(fieldDelimiter.ToString(CultureInfo.CurrentCulture));
            xmlTextWriter.WriteEndElement();
            xmlTextWriter.WriteStartElement("QuoteChar");
            xmlTextWriter.WriteCData(quoteChar.ToString(CultureInfo.CurrentCulture));
            xmlTextWriter.WriteEndElement();
            xmlTextWriter.WriteElementString("SourceFirstRowContainsColumnNames", SourceFirstRowContainsColumnNames.ToString(CultureInfo.CurrentCulture));
            xmlTextWriter.WriteElementString("DestinationFirstRowContainsColumnNames", DestinationFirstRowContainsColumnNames.ToString(CultureInfo.CurrentCulture));
            xmlTextWriter.WriteElementString("SourcePath", _path);
            xmlTextWriter.WriteElementString("SourceFile", SourceFile);
            if (!string.IsNullOrEmpty(SourceEncoding))
                xmlTextWriter.WriteElementString("Encoding", SourceEncoding);
            if (!string.IsNullOrEmpty(DestinationEncoding))
                xmlTextWriter.WriteElementString("DestinationEncoding", DestinationEncoding);
            xmlTextWriter.WriteElementString("SourceDecimalSeparator", sourceDecimalSeparator);
            xmlTextWriter.WriteElementString("ExportCultureInfo", ExportCultureInfo);
            xmlTextWriter.WriteElementString("DeleteSourceFiles", DeleteSourceFiles.ToString());
            xmlTextWriter.WriteElementString("IncludeTimestampInFileName", IncludeTimestampInFileName.ToString(CultureInfo.CurrentCulture));
            xmlTextWriter.WriteElementString("IgnoreDefectiveRows", IgnoreDefectiveRows.ToString(CultureInfo.CurrentCulture));
            GetSchema().SaveAsXml(xmlTextWriter);
        }
        public CsvProvider()
        {
        }

        public override void Close()
        {
            if (DeleteSourceFiles)
            {
                DeleteFiles();
            }
        }

        public CsvProvider(XmlNode xmlNode)
        {
            SourceDecimalSeparator = NoneDecimalSeparator;

            foreach (XmlNode node in xmlNode.ChildNodes)
            {
                switch (node.Name)
                {
                    case "SourceFieldDelimiter":
                        SourceFieldDelimiter = Convert.ToChar(node.FirstChild.Value, CultureInfo.CurrentCulture);
                        break;
                    case "DestinationFieldDelimiter":
                        DestinationFieldDelimiter = Convert.ToChar(node.FirstChild.Value, CultureInfo.CurrentCulture);
                        break;
                    case "QuoteChar":
                        quoteChar = Convert.ToChar(node.FirstChild.Value, CultureInfo.CurrentCulture);
                        break;
                    case "Schema":
                        schema = new Schema(node);
                        break;
                    case "SourcePath":
                        SourcePath = node.FirstChild.Value;
                        break;
                    case "SourceFile":
                        if (node.HasChildNodes)
                        {
                            SourceFile = node.FirstChild.Value;
                        }
                        break;
                    case "DestinationPath":
                        DestinationPath = node.FirstChild.Value;
                        break;
                    case "SourceFirstRowContainsColumnNames":
                        SourceFirstRowContainsColumnNames = node.FirstChild.Value == "True";
                        break;
                    case "DestinationFirstRowContainsColumnNames":
                        DestinationFirstRowContainsColumnNames = node.FirstChild.Value == "True";
                        break;
                    case "Encoding":
                        SourceEncoding = node.FirstChild.Value;
                        break;
                    case "DestinationEncoding":
                        DestinationEncoding = node.FirstChild.Value;
                        break;
                    case "SourceDecimalSeparator":
                        if (node.HasChildNodes)
                            sourceDecimalSeparator = node.FirstChild.Value;
                        break;
                    case "ExportCultureInfo":
                        if (node.HasChildNodes)
                            ExportCultureInfo = node.FirstChild.Value;
                        break;
                    case "DeleteSourceFiles":
                        if (node.HasChildNodes)
                            DeleteSourceFiles = node.FirstChild.Value == "True";
                        break;
                    case "IncludeTimestampInFileName":
                        if (node.HasChildNodes)
                            IncludeTimestampInFileName = node.FirstChild.Value == "True";
                        break;
                    case "IgnoreDefectiveRows":
                        if (node.HasChildNodes)
                            IgnoreDefectiveRows = node.FirstChild.Value == "True";
                        break;
                }
            }
        }

        internal CsvProvider(Dictionary<string, CsvReader> csvReaders, Schema schema, List<CsvDestinationWriter> writer)
        {
            csvReadersForTest = csvReaders;
            this.schema = schema;
            destinationWriters = writer;
        }
        public CsvProvider(string path)
        {
            this._path = path;
        }

        public new ISourceReader GetReader(Mapping mapping)
        {
            bool autoDetectDecimalSeparator = (sourceDecimalSeparator == null) ? false : (sourceDecimalSeparator == DetectAutomaticallySeparator);
            string decimalSeparator = null;
            //If source decimal separator is diffent from current culture separator - use source decimal separator
            if (!autoDetectDecimalSeparator && !string.IsNullOrEmpty(sourceDecimalSeparator) && sourceDecimalSeparator != NoneDecimalSeparator &&
                sourceDecimalSeparator != CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator)
                decimalSeparator = sourceDecimalSeparator;

            string currentPath = _path;
            string filePath;
            if (!string.IsNullOrEmpty(workingDirectory))
                currentPath = currentPath.CombinePaths((workingDirectory.CombinePaths(_path)).Replace("\\", "/"));
            currentPath = workingDirectory.CombinePaths(_path).Replace("\\", "/");
            if (!string.IsNullOrEmpty(SourceFile))
            {
                if (SourceFile.StartsWith(".."))
                {
                    filePath = (workingDirectory.CombinePaths(SourceFile.TrimStart(new char[] { '.' })).Replace("\\", "/"));
                }
                else
                {
                    filePath = currentPath.CombinePaths(SourceFile);
                }
            }
            else
            {
                filePath = currentPath.CombinePaths(mapping.SourceTable.Name + ".csv");
            }

            return new CsvSourceReader(filePath, mapping, SourceFirstRowContainsColumnNames,
                fieldDelimiter, quoteChar, GetEncoding(SourceEncoding), decimalSeparator, autoDetectDecimalSeparator, IgnoreDefectiveRows, Logger);
        }

        public override void LoadSettings(Job job)
        {
            CheckSourceFilesChanging();
        }

        public override string Serialize()
        {
            XDocument document = new XDocument(new XDeclaration("1.0", "utf-8", string.Empty));
            XElement root = new XElement("Parameters");
            document.Add(root);
            root.Add(CreateParameterNode(GetType(), "First row in source files contains column names", SourceFirstRowContainsColumnNames.ToString(CultureInfo.CurrentCulture)));
            root.Add(CreateParameterNode(GetType(), "First row in destination files shall contain column names", DestinationFirstRowContainsColumnNames.ToString(CultureInfo.CurrentCulture)));
            root.Add(CreateParameterNode(GetType(), "Source folder", SourcePath));
            root.Add(CreateParameterNode(GetType(), "Source file", SourceFile));
            root.Add(CreateParameterNode(GetType(), "Destination folder", DestinationPath));
            root.Add(CreateParameterNode(GetType(), "Input Field delimiter", fieldDelimiter.ToString(CultureInfo.CurrentCulture)));
            root.Add(CreateParameterNode(GetType(), "Output Field delimiter", fieldDelimiter.ToString(CultureInfo.CurrentCulture)));
            root.Add(CreateParameterNode(GetType(), "Input string delimiter", SourceQuoteCharacter.ToString(CultureInfo.CurrentCulture)));
            root.Add(CreateParameterNode(GetType(), "Output string delimiter", DestinationQuoteCharacter.ToString(CultureInfo.CurrentCulture)));
            root.Add(CreateParameterNode(GetType(), "Source encoding", SourceEncoding));
            root.Add(CreateParameterNode(GetType(), "Destination encoding", DestinationEncoding));
            root.Add(CreateParameterNode(GetType(), "Source decimal separator", sourceDecimalSeparator));
            root.Add(CreateParameterNode(GetType(), "Number format culture", ExportCultureInfo));
            root.Add(CreateParameterNode(GetType(), "Delete source files", DeleteSourceFiles.ToString()));
            root.Add(CreateParameterNode(GetType(), "Include timestamp in filename", IncludeTimestampInFileName.ToString(CultureInfo.CurrentCulture)));
            root.Add(CreateParameterNode(GetType(), "Ignore defective rows", IgnoreDefectiveRows.ToString(CultureInfo.CurrentCulture)));
            return document.ToString();
        }
        public override void UpdateSourceSettings(ISource source)
        {
            CsvProvider newProvider = (CsvProvider)source;
            SourceFirstRowContainsColumnNames = newProvider.SourceFirstRowContainsColumnNames;
            DestinationFirstRowContainsColumnNames = newProvider.DestinationFirstRowContainsColumnNames;
            _path = newProvider._path;
            fieldDelimiter = newProvider.fieldDelimiter;
            quoteChar = newProvider.quoteChar;
            SourceEncoding = newProvider.SourceEncoding;
            DestinationEncoding = newProvider.DestinationEncoding;
            SourceDecimalSeparator = newProvider.SourceDecimalSeparator;
            ExportCultureInfo = newProvider.ExportCultureInfo;
            DeleteSourceFiles = newProvider.DeleteSourceFiles;
            IncludeTimestampInFileName = newProvider.IncludeTimestampInFileName;
            IgnoreDefectiveRows = newProvider.IgnoreDefectiveRows;
            SourceFile = newProvider.SourceFile;
        }
        public override void UpdateDestinationSettings(IDestination destination)
        {
            ISource newProvider = (ISource)destination;
            UpdateSourceSettings(newProvider);
        }

        public override bool RunJob(Job job)
        {
            ReplaceMappingConditionalsWithValuesFromRequest(job);
            Dictionary<string, object> sourceRow = null;
            try
            {
                CultureInfo ci = GetCultureInfo();

                if (destinationWriters == null)
                {
                    destinationWriters = new List<CsvDestinationWriter>();
                    foreach (var mapping in job.Mappings)
                    {
                        if (mapping.Active && mapping.GetColumnMappings().Count > 0)
                        {
                            if (!string.IsNullOrEmpty(WorkingDirectory))
                            {
                                destinationWriters.Add(new CsvDestinationWriter((workingDirectory.CombinePaths(_path)).Replace("\\", "/"), mapping, DestinationFirstRowContainsColumnNames, fieldDelimiter, quoteChar, GetEncoding(DestinationEncoding), ci, IncludeTimestampInFileName));
                            }
                            else
                            {
                                destinationWriters.Add(new CsvDestinationWriter(_path, mapping, DestinationFirstRowContainsColumnNames, fieldDelimiter, quoteChar, GetEncoding(DestinationEncoding), ci, IncludeTimestampInFileName));
                            }
                        }
                    }
                }

                foreach (var writer in destinationWriters)
                {
                    using (ISourceReader sourceReader = writer.Mapping.Source.GetReader(writer.Mapping))
                    {
                        while (!sourceReader.IsDone())
                        {
                            sourceRow = sourceReader.GetNext();
                            ProcessInputRow(writer.Mapping, sourceRow);
                            writer.Write(sourceRow);
                        }
                    }

                }
                sourceRow = null;
            }
            catch (Exception ex)
            {
                string msg = ex.Message;
                LogManager.System.GetLogger(LogCategory.Application, "Dataintegration").Error($"{GetType().Name} error: {ex.Message} Stack: {ex.StackTrace}", ex);

                if (sourceRow != null)
                    msg += GetFailedSourceRowMessage(sourceRow);

                Logger?.Log("Job failed: " + msg);
                return false;
            }
            finally
            {
                foreach (var writer in destinationWriters)
                {
                    writer.Close();
                }
            }
            return true;
        }

        public Hashtable GetOptions(string name)
        {
            if (name == "Source decimal separator")
            {
                var options = new Hashtable
                              {
                                  {NoneDecimalSeparator, NoneDecimalSeparator},
                                  {DetectAutomaticallySeparator, DetectAutomaticallySeparator},
                                  {".", "."},
                                  {",", ","}
                              };
                return options;
            }
            else if (name == "Number format culture")
            {
                var options = new Hashtable();
                CountryCollection countries = Ecommerce.Services.Countries.GetCountries();
                foreach (Country c in countries)
                {
                    if (!string.IsNullOrEmpty(c.CultureInfo) && !options.Contains(c.CultureInfo))
                        options.Add(c.CultureInfo, c.Code2);
                }
                return options;
            }
            else
            {
                var options = new Hashtable
                              {
                                  {"UTF8", "Unicode (UTF-8)"},
                                  {"UTF16", "Unicode (UTF-16)"},
                                  {"1252", "Windows-1252 (default legacy components of Microsoft Windows. English and most of Europe)"},
                                  {"1251", "Windows-1251 (covering cyrillic, Eastern Europe)"}
                              };
                return options;
            }
        }

        private Encoding GetEncoding(string encoding)
        {
            Encoding result = Encoding.UTF8;
            if (!string.IsNullOrEmpty(encoding))
            {
                if (encoding.Contains("1252"))
                    result = Encoding.GetEncoding(1252);
                if (encoding.Contains("1251"))
                    result = Encoding.GetEncoding(1251);
                if (encoding.Contains("UTF16"))
                    result = Encoding.Unicode;
            }
            return result;
        }

        private CultureInfo GetCultureInfo()
        {
            CultureInfo result = null;

            if (!string.IsNullOrEmpty(ExportCultureInfo))
            {
                try
                {
                    result = CultureInfo.GetCultureInfo(ExportCultureInfo);
                }
                catch (Exception ex)
                {
                    if (Logger != null)
                        Logger.Log(string.Format("Error getting culture: {0}.", ex.Message));
                }
            }

            return result;
        }

        private IEnumerable<string> GetSourceFiles()
        {
            IEnumerable<string> files = new List<string>();

            string currentPath = _path;
            if (!string.IsNullOrEmpty(workingDirectory))
                currentPath = (workingDirectory.CombinePaths(_path)).Replace("\\", "/");
            if (!string.IsNullOrEmpty(SourceFile))
            {
                if (SourceFile.StartsWith(".."))
                {
                    files = new List<string>() { (workingDirectory.CombinePaths(SourceFile.TrimStart(new char[] { '.' })).Replace("\\", "/")) };
                }
                else
                {
                    files = new List<string>() { currentPath.CombinePaths(SourceFile) };
                }
            }
            else
            {
                if (File.Exists(currentPath))
                {
                    files = new List<string>() { currentPath };
                }
                if (Directory.Exists(currentPath))
                {
                    files = Directory.EnumerateFiles(currentPath, "*.csv", SearchOption.TopDirectoryOnly);
                }
            }
            return files;
        }

        private void DeleteFiles()
        {
            foreach (string file in GetSourceFiles())
            {
                try
                {
                    File.Delete(file);
                }
                catch (Exception ex)
                {
                    Logger?.Log(string.Format("Can't delete source file: {0}. Error: {1}", file, ex.Message));
                }
            }
        }

        private void CheckSourceFilesChanging()
        {
            IEnumerable<string> files = GetSourceFiles().Distinct();
            if (files != null && files.Count() > 0)
            {
                Logger?.Log("Start checking input files changing");

                Dictionary<string, long> fileSizeDictionary = new Dictionary<string, long>();
                foreach (string file in files)
                {
                    if (File.Exists(file))
                    {
                        FileInfo fileInfo = new FileInfo(file);
                        fileSizeDictionary.Add(file, fileInfo.Length);
                    }
                }
                System.Threading.Thread.Sleep(5 * 1000);
                foreach (string file in fileSizeDictionary.Keys)
                {
                    FileInfo changedFileInfo = new FileInfo(file);
                    if (changedFileInfo != null && changedFileInfo.Length != fileSizeDictionary[file])
                    {
                        throw new Exception(string.Format("Source file: '{0}' is still updating", file));
                    }
                }
                Logger?.Log("Finish checking input files changing");
            }
        }

        public void WriteToSourceFile(string InputXML)
        {
            WorkingDirectory = SystemInformation.MapPath("/Files/");
            if (!string.IsNullOrEmpty(SourceFile))
            {
                var filePath = WorkingDirectory.CombinePaths(SourceFile);
                try
                {
                    File.WriteAllText(filePath, InputXML);
                }
                catch (Exception)
                {
                    TextFileHelper.WriteTextFile(InputXML, filePath);
                }
            }
        }
    }
}