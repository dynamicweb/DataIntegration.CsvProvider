using Dynamicweb.DataIntegration.Integration;
using System;

namespace Dynamicweb.DataIntegration.Providers.CsvProvider
{    
    internal static class MappingExtensions
    {
        // Obsoleted. Use DataIntegration ColumnMapping.HasScriptWithValue instead
        internal static bool HasScriptWithValue(this ColumnMapping columnMapping)
        {
            return columnMapping.ScriptType == ScriptType.Constant || columnMapping.HasNewGuidScript();
        }

        // Obsoleted. Use DataIntegration ScriptType.NewGuid instead
        internal static bool HasNewGuidScript(this ColumnMapping columnMapping)
        {
            return Enum.IsDefined(typeof(ScriptType), "NewGuid") && (int)columnMapping.ScriptType == 4;
        }

        // Obsoleted. Use DataIntegration ColumnMapping.GetScriptValue instead
        public static string GetScriptValue(this ColumnMapping columnMapping)
        {
            if (columnMapping.ScriptType == ScriptType.Constant)
                return columnMapping.ScriptValue;
            if (columnMapping.HasNewGuidScript())
                return Guid.NewGuid().ToString();
            return null;
        }
    }
}
