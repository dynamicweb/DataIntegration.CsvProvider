using CsvHelper;
using CsvHelper.Configuration;
using Dynamicweb.Core;
using Dynamicweb.Core.Helpers;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.Extensibility.AddIns;
using Dynamicweb.Extensibility.Editors;
using Dynamicweb.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Linq;

namespace Dynamicweb.DataIntegration.Providers.CsvProvider;

[AddInName("Dynamicweb.DataIntegration.Providers.Provider"), AddInLabel("CSV Provider"), AddInDescription("CSV provider"), AddInIgnore(false)]
public class CsvProvider : BaseProvider, ISource, IDestination, IParameterOptions
{
    private bool _sourceFirstRowContainsColumnNames = true;
    private bool _destinationFirstRowContainsColumnNames = true;
    private string _path = "";
    private Schema _schema;
    private readonly Dictionary<string, CsvReader> _csvReadersForTest;
    private List<CsvDestinationWriter> _destinationWriters;
    private readonly string _detectAutomaticallySeparator = "Detect automatically";
    private readonly string _noneDecimalSeparator = "Use system culture";
    private string _sourceDecimalSeparator;
    private string _workingDirectory = SystemInformation.MapPath("/Files/");
    private string _fieldDelimiter = ";";
    private string _quoteChar = "\"";

    [AddInParameter("First row in source files contains column names"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Source")]
    public virtual bool SourceFirstRowContainsColumnNames
    {
        get
        {
            return _sourceFirstRowContainsColumnNames;
        }
        set
        {
            _sourceFirstRowContainsColumnNames = value;
        }
    }

    [AddInParameter("First row in destination files shall contain column names"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Destination")]
    public virtual bool DestinationFirstRowContainsColumnNames
    {
        get
        {
            return _destinationFirstRowContainsColumnNames;
        }
        set
        {
            _destinationFirstRowContainsColumnNames = value;
        }
    }

    [AddInParameter("Include timestamp in filename"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Destination")]
    public virtual bool IncludeTimestampInFileName { get; set; }

    //path should point to a folder - if it doesn't, write will fail.
    [AddInParameter("Source folder"), AddInParameterEditor(typeof(FolderSelectEditor), "folder=/Files/;"), AddInParameterGroup("Source")]
    public string SourcePath
    {
        get { return _path; }
        set { _path = value; }
    }

    [AddInParameter("Source file"), AddInParameterEditor(typeof(FileManagerEditor), "folder=/Files/;Tooltip=Selecting a source file will override source folder selection"), AddInParameterGroup("Source")]
    public string SourceFile { get; set; }

    [AddInParameter("Delete source files"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Source")]
    public bool DeleteSourceFiles { get; set; }

    [AddInParameter("Destination folder"), AddInParameterEditor(typeof(FolderSelectEditor), "folder=/Files/;"), AddInParameterGroup("Destination")]
    public string DestinationPath
    {
        get { return _path; }
        set { _path = value; }
    }

    [AddInParameter("Input Field delimiter"), AddInParameterEditor(typeof(TextParameterEditor), "maxLength=1"), AddInParameterGroup("Source")]
    public string SourceFieldDelimiter
    {
        get { return _fieldDelimiter; }
        set { _fieldDelimiter = value; }
    }

    [AddInParameter("Output Field delimiter"), AddInParameterEditor(typeof(TextParameterEditor), "maxLength=1"), AddInParameterGroup("Destination")]
    public string DestinationFieldDelimiter
    {
        get { return _fieldDelimiter; }
        set { _fieldDelimiter = value; }
    }

    [AddInParameter("Input string delimiter"), AddInParameterEditor(typeof(TextParameterEditor), "maxLength=1"), AddInParameterGroup("Source")]
    public string SourceQuoteCharacter
    {
        get { return _quoteChar; }
        set { _quoteChar = value; }
    }

    [AddInParameter("Output string delimiter"), AddInParameterEditor(typeof(TextParameterEditor), "maxLength=1"), AddInParameterGroup("Destination")]
    public string DestinationQuoteCharacter
    {
        get { return _quoteChar; }
        set { _quoteChar = value; }
    }

    [AddInParameter("Source encoding"), AddInParameterEditor(typeof(DropDownParameterEditor), "none=true"), AddInParameterGroup("Source")]
    public string SourceEncoding { get; set; }

    [AddInParameter("Destination encoding"), AddInParameterEditor(typeof(DropDownParameterEditor), "none=true"), AddInParameterGroup("Destination")]
    public string DestinationEncoding { get; set; }

    [AddInParameter("Number format culture"), AddInParameterEditor(typeof(DropDownParameterEditor), "none=true"), AddInParameterGroup("Destination")]
    public string ExportCultureInfo { get; set; }

    [AddInParameter("Source decimal separator"), AddInParameterEditor(typeof(DropDownParameterEditor), "none=false"), AddInParameterGroup("Source")]
    public string SourceDecimalSeparator
    {
        get
        {
            if (string.IsNullOrEmpty(_sourceDecimalSeparator))
            {
                return _detectAutomaticallySeparator;
            }
            else
            {
                return _sourceDecimalSeparator;
            }
        }
        set { _sourceDecimalSeparator = value; }
    }

    [AddInParameter("Skip Troublesome rows"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Source")]
    public bool IgnoreDefectiveRows { get; set; }

    public override string WorkingDirectory
    {
        get
        {
            return _workingDirectory;
        }
        set { _workingDirectory = value.Replace("\\", "/"); }
    }

    public override bool SchemaIsEditable
    {
        get { return true; }
    }

    public override Schema GetOriginalSourceSchema()
    {
        Schema result = new Schema();

        if (_csvReadersForTest != null && _csvReadersForTest.Count > 0)//if unit tests are executing
        {
            GetSchemaForTableFromFile(result, _csvReadersForTest);
        }
        else
        {
            Dictionary<string, CsvReader> csvReaders = new Dictionary<string, CsvReader>();
            var currentEncoding = string.IsNullOrEmpty(SourceEncoding) ? Encoding.UTF8 : GetEncoding(SourceEncoding);
            var config = new CsvConfiguration(CultureInfo.CurrentCulture)
            {
                Comment = '¤',
                Delimiter = _fieldDelimiter + "",
                Encoding = currentEncoding,
                HasHeaderRecord = SourceFirstRowContainsColumnNames,
                Escape = '\\',                    
                TrimOptions = TrimOptions.None,
                DetectColumnCountChanges = true
            };
            if (!string.IsNullOrEmpty(_quoteChar))
            {
                config.Quote = Convert.ToChar(_quoteChar, CultureInfo.CurrentCulture);
            }

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
        _schema = GetOriginalSourceSchema();
    }

    public override void OverwriteDestinationSchemaToOriginal()
    {
    }

    public override Schema GetSchema()
    {
        if (_schema == null)
        {
            _schema = GetOriginalSourceSchema();
        }
        return _schema;
    }

    public override string ValidateDestinationSettings()
    {
        if (!Directory.Exists((WorkingDirectory + DestinationPath).Replace("\\", "/")))
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
            string srcFolderPath = WorkingDirectory.CombinePaths(SourcePath).Replace("\\", "/");

            if (!Directory.Exists(srcFolderPath))
            {
                return "Source folder \"" + SourcePath + "\" does not exist";
            }
            else
            {
                var files = Directory.GetFiles(srcFolderPath);

                if (files.Count() == 0)
                {
                    return "There are no files in the source folder";
                }
            }
        }
        else
        {
            if (!File.Exists(GetSourceFile()))
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
                    reader.Key, _fieldDelimiter, csvEx.Message);
                Logger?.Log(msg);
                schema.RemoveTable(csvTable);
            }
        }
    }

    public new virtual void SaveAsXml(XmlTextWriter xmlTextWriter)
    {
        xmlTextWriter.WriteStartElement("SourceFieldDelimiter");
        xmlTextWriter.WriteCData(_fieldDelimiter.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteEndElement();
        xmlTextWriter.WriteStartElement("QuoteChar");
        xmlTextWriter.WriteCData(_quoteChar?.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteEndElement();
        xmlTextWriter.WriteElementString("SourceFirstRowContainsColumnNames", SourceFirstRowContainsColumnNames.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("DestinationFirstRowContainsColumnNames", DestinationFirstRowContainsColumnNames.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("SourcePath", _path);
        xmlTextWriter.WriteElementString("SourceFile", SourceFile);
        if (!string.IsNullOrEmpty(SourceEncoding))
            xmlTextWriter.WriteElementString("Encoding", SourceEncoding);
        if (!string.IsNullOrEmpty(DestinationEncoding))
            xmlTextWriter.WriteElementString("DestinationEncoding", DestinationEncoding);
        xmlTextWriter.WriteElementString("SourceDecimalSeparator", _sourceDecimalSeparator);
        xmlTextWriter.WriteElementString("ExportCultureInfo", ExportCultureInfo);
        xmlTextWriter.WriteElementString("DeleteSourceFiles", DeleteSourceFiles.ToString());
        xmlTextWriter.WriteElementString("IncludeTimestampInFileName", IncludeTimestampInFileName.ToString(CultureInfo.CurrentCulture));
        xmlTextWriter.WriteElementString("IgnoreDefectiveRows", IgnoreDefectiveRows.ToString(CultureInfo.CurrentCulture));
        GetSchema().SaveAsXml(xmlTextWriter);
    }

    public CsvProvider() { }

    public override void Close()
    {
        if (DeleteSourceFiles)
        {
            DeleteFiles();
        }
    }

    public CsvProvider(XmlNode xmlNode)
    {
        SourceDecimalSeparator = _noneDecimalSeparator;

        foreach (XmlNode node in xmlNode.ChildNodes)
        {
            switch (node.Name)
            {
                case "SourceFieldDelimiter":
                    if (node.HasChildNodes)
                    {
                        SourceFieldDelimiter = node.FirstChild.Value;
                    }
                    break;
                case "DestinationFieldDelimiter":
                    if (node.HasChildNodes)
                    {
                        DestinationFieldDelimiter = node.FirstChild.Value;
                    }
                    break;
                case "QuoteChar":
                    if (node.HasChildNodes)
                    {
                        _quoteChar = node.FirstChild?.Value;
                    }
                    break;
                case "Schema":
                    _schema = new Schema(node);
                    break;
                case "SourcePath":
                    if (node.HasChildNodes)
                    {
                        SourcePath = node.FirstChild.Value;
                    }
                    break;
                case "SourceFile":
                    if (node.HasChildNodes)
                    {
                        SourceFile = node.FirstChild.Value;
                    }
                    break;
                case "DestinationPath":
                    if (node.HasChildNodes)
                    {
                        DestinationPath = node.FirstChild.Value;
                    }
                    break;
                case "SourceFirstRowContainsColumnNames":
                    if (node.HasChildNodes)
                    {
                        SourceFirstRowContainsColumnNames = node.FirstChild.Value == "True";
                    }
                    break;
                case "DestinationFirstRowContainsColumnNames":
                    if (node.HasChildNodes)
                    {
                        DestinationFirstRowContainsColumnNames = node.FirstChild.Value == "True";
                    }
                    break;
                case "Encoding":
                    if (node.HasChildNodes)
                    {
                        SourceEncoding = node.FirstChild.Value;
                    }
                    break;
                case "DestinationEncoding":
                    if (node.HasChildNodes)
                    {
                        DestinationEncoding = node.FirstChild.Value;
                    }
                    break;
                case "SourceDecimalSeparator":
                    if (node.HasChildNodes)
                    {
                        _sourceDecimalSeparator = node.FirstChild.Value;
                    }
                    break;
                case "ExportCultureInfo":
                    if (node.HasChildNodes)
                    {
                        ExportCultureInfo = node.FirstChild.Value;
                    }
                    break;
                case "DeleteSourceFiles":
                    if (node.HasChildNodes)
                    {
                        DeleteSourceFiles = node.FirstChild.Value == "True";
                    }
                    break;
                case "IncludeTimestampInFileName":
                    if (node.HasChildNodes)
                    {
                        IncludeTimestampInFileName = node.FirstChild.Value == "True";
                    }
                    break;
                case "IgnoreDefectiveRows":
                    if (node.HasChildNodes)
                    {
                        IgnoreDefectiveRows = node.FirstChild.Value == "True";
                    }
                    break;
            }
        }
    }

    internal CsvProvider(Dictionary<string, CsvReader> csvReaders, Schema schema, List<CsvDestinationWriter> writer)
    {
        _csvReadersForTest = csvReaders;
        this._schema = schema;
        _destinationWriters = writer;
    }

    public CsvProvider(string path)
    {
        this._path = path;
    }

    public new ISourceReader GetReader(Mapping mapping)
    {
        bool autoDetectDecimalSeparator = (_sourceDecimalSeparator == null) ? false : (_sourceDecimalSeparator == _detectAutomaticallySeparator);
        string decimalSeparator = null;
        //If source decimal separator is diffent from current culture separator - use source decimal separator
        if (!autoDetectDecimalSeparator && !string.IsNullOrEmpty(_sourceDecimalSeparator) && _sourceDecimalSeparator != _noneDecimalSeparator &&
            _sourceDecimalSeparator != CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator)
            decimalSeparator = _sourceDecimalSeparator;

        string filePath;
        if (!string.IsNullOrEmpty(SourceFile))
        {
            filePath = GetSourceFile();
        }
        else
        {
            filePath = WorkingDirectory.CombinePaths(_path).Replace("\\", "/").CombinePaths(mapping.SourceTable.Name + ".csv");
        }

        return new CsvSourceReader(filePath, mapping, SourceFirstRowContainsColumnNames,
            Convert.ToChar(_fieldDelimiter, CultureInfo.CurrentCulture), !string.IsNullOrEmpty(_quoteChar) ? Convert.ToChar(_quoteChar, CultureInfo.CurrentCulture) : char.MinValue
            , GetEncoding(SourceEncoding), decimalSeparator, autoDetectDecimalSeparator, IgnoreDefectiveRows, Logger, this);
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
        root.Add(CreateParameterNode(GetType(), "Input Field delimiter", _fieldDelimiter.ToString(CultureInfo.CurrentCulture)));
        root.Add(CreateParameterNode(GetType(), "Output Field delimiter", _fieldDelimiter.ToString(CultureInfo.CurrentCulture)));
        root.Add(CreateParameterNode(GetType(), "Input string delimiter", SourceQuoteCharacter.ToString(CultureInfo.CurrentCulture)));
        root.Add(CreateParameterNode(GetType(), "Output string delimiter", DestinationQuoteCharacter.ToString(CultureInfo.CurrentCulture)));
        root.Add(CreateParameterNode(GetType(), "Source encoding", SourceEncoding));
        root.Add(CreateParameterNode(GetType(), "Destination encoding", DestinationEncoding));
        root.Add(CreateParameterNode(GetType(), "Source decimal separator", _sourceDecimalSeparator));
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
        _fieldDelimiter = newProvider._fieldDelimiter;
        _quoteChar = newProvider._quoteChar;
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

                if (_destinationWriters == null)
                {
                    _destinationWriters = new List<CsvDestinationWriter>();
                    foreach (var mapping in job.Mappings)
                    {
                        if (mapping.Active && mapping.GetColumnMappings().Count > 0)
                        {
                            _destinationWriters.Add(new CsvDestinationWriter((WorkingDirectory.CombinePaths(_path)).Replace("\\", "/"), mapping, DestinationFirstRowContainsColumnNames, Convert.ToChar(_fieldDelimiter, CultureInfo.CurrentCulture), Convert.ToChar(_quoteChar, CultureInfo.CurrentCulture), GetEncoding(DestinationEncoding), ci, IncludeTimestampInFileName));
                        }
                    }
                }

                foreach (var writer in _destinationWriters)
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
            string stackTrace = ex.StackTrace;
            Logger?.Error($"Error: {msg.Replace(System.Environment.NewLine, " ")} Stack: {stackTrace.Replace(System.Environment.NewLine, " ")}", ex);
            LogManager.System.GetLogger(LogCategory.Application, "Dataintegration").Error($"{GetType().Name} error: {msg} Stack: {stackTrace}", ex);

            if (sourceRow != null)
                msg += GetFailedSourceRowMessage(sourceRow);

                Logger?.Log("Job failed: " + msg);
                return false;
            }
            finally
            {
                foreach (var writer in _destinationWriters)
                {
                    writer.Close();
                }
            }
            return true;
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

    private string GetSourceFile()
    {
        var filePath = WorkingDirectory.CombinePaths(SourceFile).Replace("\\", "/");
        if (File.Exists(filePath))
        {
            return filePath;
        }
        return string.Empty;
    }

    private IEnumerable<string> GetSourceFiles()
    {
        if (!string.IsNullOrEmpty(SourceFile))
        {
            return new[] { GetSourceFile() };
        }
        else
        {
            string currentPath = WorkingDirectory.CombinePaths(_path).Replace("\\", "/");
            if (Directory.Exists(currentPath))
            {
                return Directory.EnumerateFiles(currentPath, "*.csv", SearchOption.TopDirectoryOnly);
            }
            return Enumerable.Empty<string>();
        }
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

    public IEnumerable<ParameterOption> GetParameterOptions(string parameterName) => parameterName switch
    {
        "Source decimal separator" => new List<ParameterOption>()
                {
                    new(_noneDecimalSeparator, _noneDecimalSeparator),
                    new(_detectAutomaticallySeparator, _detectAutomaticallySeparator),
                    new(".", "."),
                    new(",", ",")
                },
        "Number format culture" => Ecommerce.Services.Countries.GetCountries()
                .Where(c => !string.IsNullOrEmpty(c.CultureInfo))
                .DistinctBy(c => c.CultureInfo)
                .Select(c => new ParameterOption(c.Code2, c.CultureInfo)),
        _ => new List<ParameterOption>()
                {
                    new("Unicode (UTF-8)", "UTF8"),
                    new("Unicode (UTF-16)", "UTF16"),
                    new("Windows-1252 (default legacy components of Microsoft Windows. English and most of Europe)", "1252"),
                    new("Windows-1251 (covering cyrillic, Eastern Europe)", "1251")
                },
    };
}
