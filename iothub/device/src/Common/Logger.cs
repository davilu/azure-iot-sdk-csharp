using System;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;

namespace Microsoft.Azure.Devices.Client
{
    internal static class Logger
    {
        internal const string s_timeFormat = "yyyy-MM-dd HH:mm:ss.ffff";

        internal static readonly bool IsEnabled = true;
        internal static readonly string s_logFilePat = Path.Combine(Path.GetTempPath(), $"SDK-{DateTime.UtcNow:yyyy-MM-dd-HHmmss}.log");
        internal static readonly object s_lock = new object();
        internal static StreamWriter s_streamWriter;

        private static void WriteLog(string log)
        {
            string timestamp = DateTime.Now.ToString(s_timeFormat, CultureInfo.InvariantCulture);
            lock (s_lock)
            {
                if (s_streamWriter == null)
                {
                    Console.WriteLine($"Log file: {s_logFilePat}");
#if NETSTANDARD1_3
                    FileInfo fileInfo = new FileInfo(s_logFilePath);
                    if (!fileInfo.Exists)
                    {
                        s_streamWriter = fileInfo.CreateText();
                    }
                    else
                    {
                        s_streamWriter = fileInfo.AppendText();
                    }
#else
                    s_streamWriter = new StreamWriter(s_logFilePat, true);
#endif
                }
                s_streamWriter.WriteLine($"{timestamp}: {log}");
            }
        }

        internal static void Enter(object thisOrContextObject, object arg0, [CallerMemberName] string memberName = null)
        {
            if (IsEnabled) WriteLog($"Enter {thisOrContextObject} method {memberName} with args: {arg0}.");
        }

        internal static void Enter(object thisOrContextObject, object arg0, object arg1, [CallerMemberName] string memberName = null)
        {
            if (IsEnabled) WriteLog($"Enter {thisOrContextObject} method {memberName} with args: [{arg0}, {arg1}].");
        }

        internal static void Enter(object thisOrContextObject, object arg0, object arg1, object arg2, [CallerMemberName] string memberName = null)
        {
            if (IsEnabled) WriteLog($"Enter {thisOrContextObject} method {memberName} with args: [{arg0}, {arg1}, {arg2}].");
        }

        internal static void Exit(object thisOrContextObject, object arg0, [CallerMemberName] string memberName = null)
        {
            if (IsEnabled) WriteLog($"Exit {thisOrContextObject} method {memberName} with args: {arg0}.");
        }

        internal static void Exit(object thisOrContextObject, object arg0, object arg1, [CallerMemberName] string memberName = null)
        {
            if (IsEnabled) WriteLog($"Exit {thisOrContextObject} method {memberName} with args: [{arg0}, {arg1}].");
        }

        internal static void Exit(object thisOrContextObject, object arg0, object arg1, object arg2, [CallerMemberName] string memberName = null)
        {
            if (IsEnabled) WriteLog($"Exit {thisOrContextObject} method {memberName} with args: [{arg0}, {arg1}, {arg2}].");
        }

        internal static void Info(object thisOrContextObject, object message, [CallerMemberName] string memberName = null)
        {
            if (IsEnabled) WriteLog($"Info {thisOrContextObject} method {memberName} {message}.");
        }

        internal static void Error(object thisOrContextObject, object message, [CallerMemberName] string memberName = null)
        {
            if (IsEnabled) WriteLog($"Error {thisOrContextObject} method {memberName} {message}");
        }

        internal static void Associate(object thisOrContextObject, object parent, object resource, [CallerMemberName] string memberName = null)
        {
            if (IsEnabled) WriteLog($"Associate: {thisOrContextObject} method {memberName} {resource} is Associated to {parent}.");
        }

        internal static void Associate(object thisOrContextObject, object resource, [CallerMemberName] string memberName = null)
        {
            if (IsEnabled) WriteLog($"Associate: {thisOrContextObject} method {memberName} {resource} is Associated to {thisOrContextObject}.");
        }
    }
}