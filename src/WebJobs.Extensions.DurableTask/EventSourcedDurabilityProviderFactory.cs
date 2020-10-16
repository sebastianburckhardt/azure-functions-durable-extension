// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System;
using System.IO;
using System.Threading;
using DurableTask.Core;
using DurableTask.EventSourced;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace Microsoft.Azure.WebJobs.Extensions.DurableTask
{
    internal class EventSourcedDurabilityProviderFactory : IDurabilityProviderFactory
    {
        private readonly Entry entry;

        // If running in test environment, we keep a service running and cache it in a static variable.
        // Also, we delete previous taskhub before first run.
        private static Entry cachedTestEntry;

        private readonly DurableTaskOptions options;
        private readonly EventSourcedOrchestrationServiceSettings eventSourcedSettings;
        private readonly IConnectionStringResolver connectionStringResolver;
        private readonly bool runningInTestEnvironment;
        private readonly bool traceToConsole;
        private readonly bool traceToEtwExtension;
        private readonly bool traceToBlob;
        private readonly ILoggerFactory loggerFactory;

        // the following are boolean options that can be specified in the json,
        // but are not passed on to the backend
        public const string RunningInTestEnvironmentSetting = "RunningInTestEnvironment";
        public const string TraceToConsole = "TraceToConsole";
        public const string TraceToEtwExtension = "TraceToEtwExtension";
        public const string TraceToBlob = "TraceToBlob";

        public EventSourcedDurabilityProviderFactory(
            IOptions<DurableTaskOptions> options,
            IConnectionStringResolver connectionStringResolver,
            ILoggerFactory loggerFactory)
        {
            // for debugging
            System.Threading.Thread.Sleep(5000);

            this.options = options.Value;
            this.connectionStringResolver = connectionStringResolver;
            this.eventSourcedSettings = new EventSourcedOrchestrationServiceSettings();

            // override DTFx defaults to the defaults we want to use in DF
            this.eventSourcedSettings.ThrowExceptionOnInvalidDedupeStatus = true;

            // copy all applicable fields from both the options and the storageProvider options
            JsonConvert.PopulateObject(JsonConvert.SerializeObject(this.options), this.eventSourcedSettings);
            JsonConvert.PopulateObject(JsonConvert.SerializeObject(this.options.StorageProvider), this.eventSourcedSettings);

            bool ReadBooleanSetting(string name) => this.options.StorageProvider.TryGetValue(name, out object objValue)
                && objValue is string stringValue && bool.TryParse(stringValue, out bool boolValue) && boolValue;

            this.runningInTestEnvironment = ReadBooleanSetting(RunningInTestEnvironmentSetting);
            this.traceToConsole = ReadBooleanSetting(TraceToConsole);
            this.traceToEtwExtension = ReadBooleanSetting(TraceToEtwExtension);
            this.traceToBlob = ReadBooleanSetting(TraceToBlob);

            // resolve any indirection in the specification of the two connection strings
            this.eventSourcedSettings.StorageConnectionString = this.ResolveIndirection(
                this.eventSourcedSettings.StorageConnectionString,
                nameof(EventSourcedOrchestrationServiceSettings.StorageConnectionString));
            this.eventSourcedSettings.EventHubsConnectionString = this.ResolveIndirection(
                this.eventSourcedSettings.EventHubsConnectionString,
                nameof(EventSourcedOrchestrationServiceSettings.EventHubsConnectionString));

            // if worker id is specified in environment, it overrides the file setting
            string workerId = Environment.GetEnvironmentVariable("WorkerId");
            if (!string.IsNullOrEmpty(workerId))
            {
                this.eventSourcedSettings.WorkerId = workerId;
            }

            if (this.runningInTestEnvironment)
            {
                // use a single task hub name for all tests to allow reuse between tests with same settings
                this.options.HubName = "test-taskhub";
                this.eventSourcedSettings.KeepServiceRunning = true;
            }

            // if the taskhubname is not valid, replace it with a default
            if (!AzureStorageOptions.IsSanitizedHubName(this.options.HubName, out string sanitizedHubName))
            {
                this.options.SetDefaultHubName(sanitizedHubName);
            }

            // make sure the settings we pass on have the fields correctly set
            this.eventSourcedSettings.HubName = this.options.HubName;

            // Use a temporary logger/traceHelper because DurableTaskExtension hasn't been called yet to create one.
            this.loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            var providerFactoryName = nameof(EventSourcedDurabilityProviderFactory);
            ILogger logger = this.loggerFactory.CreateLogger(providerFactoryName);
            var traceHelper = new EndToEndTraceHelper(logger, false);
            traceHelper.ExtensionWarningEvent(this.options.HubName, string.Empty, string.Empty, $"{providerFactoryName} instantiated");

            if (this.traceToConsole || this.traceToEtwExtension || this.traceToBlob)
            {
                // capture trace events generated in the backend and redirect them to generate an ETW event, or to trace to console
                this.loggerFactory = new LoggerFactoryWrapper(this.loggerFactory, this.options.HubName, this);
            }

            if (this.runningInTestEnvironment && cachedTestEntry != null)
            {
                if (this.eventSourcedSettings.Equals(cachedTestEntry.Settings))
                {
                    // We simply use the cached orchestration service, which is still running.
                    this.entry = cachedTestEntry;
                    return;
                }

                if (cachedTestEntry.DurabilityProvider != null)
                {
                    // The service must be stopped now since we are about to start a new one with different settings
                    ((IOrchestrationService)cachedTestEntry.DurabilityProvider).StopAsync().Wait();
                }
            }

            this.entry = new Entry()
            {
                Settings = this.eventSourcedSettings,
                DurabilityProvider = new EventSourcedDurabilityProvider(new EventSourcedOrchestrationService(this.eventSourcedSettings, this.loggerFactory)),
            };

            if (this.runningInTestEnvironment)
            {
                if (cachedTestEntry == null)
                {
                    // delete the test taskhub before the first test is run
                    ((IOrchestrationService)this.entry.DurabilityProvider).DeleteAsync().Wait();
                }

                cachedTestEntry = this.entry;
            }
        }

        private string ResolveIndirection(string value, string propertyName)
        {
            string envName;
            string setting;

            if (string.IsNullOrEmpty(value))
            {
                envName = propertyName;
            }
            else if (value.StartsWith("$"))
            {
                envName = value.Substring(1);
            }
            else if (value.StartsWith("%") && value.EndsWith("%"))
            {
                envName = value.Substring(1, value.Length - 2);
            }
            else
            {
                envName = null;
            }

            if (envName != null)
            {
                setting = this.connectionStringResolver.Resolve(envName);
            }
            else
            {
                setting = value;
            }

            if (string.IsNullOrEmpty(setting))
            {
                throw new InvalidOperationException($"Could not resolve '{envName}' for required property '{propertyName}' in EventSourced storage provider settings.");
            }
            else
            {
                return setting;
            }
        }

        internal string GetDefaultStorageConnectionString() => this.entry.Settings.StorageConnectionString;

        public DurabilityProvider GetDurabilityProvider() => this.entry.DurabilityProvider;

        public DurabilityProvider GetDurabilityProvider(DurableClientAttribute attribute)
        {
            return this.entry.DurabilityProvider; // TODO consider clients for other apps
        }

        private class Entry
        {
            public EventSourcedOrchestrationServiceSettings Settings { get; set; }

            public EventSourcedDurabilityProvider DurabilityProvider { get; set; }
        }

        private class LoggerFactoryWrapper : ILoggerFactory
        {
            private readonly ILoggerFactory loggerFactory;
            private readonly EventSourcedDurabilityProviderFactory providerFactory;
            private readonly string hubName;
            private readonly BlobLogger blobLogger;

            public LoggerFactoryWrapper(ILoggerFactory loggerFactory, string hubName, EventSourcedDurabilityProviderFactory providerFactory)
            {
                this.hubName = hubName;
                this.loggerFactory = loggerFactory;
                this.providerFactory = providerFactory;

                if (providerFactory.traceToBlob)
                {
                    this.blobLogger = new BlobLogger(providerFactory);
                }
            }

            public void AddProvider(ILoggerProvider provider)
            {
                this.loggerFactory.AddProvider(provider);
            }

            public ILogger CreateLogger(string categoryName)
            {
                var logger = this.loggerFactory.CreateLogger(categoryName);
                return new LoggerWrapper(logger, categoryName, this.hubName, this.providerFactory, this.blobLogger);
            }

            public void Dispose()
            {
                this.loggerFactory.Dispose();
            }
        }

        private class LoggerWrapper : ILogger
        {
            private static readonly string ExtensionVersion = System.Diagnostics.FileVersionInfo.GetVersionInfo(typeof(DurableTaskExtension).Assembly.Location).FileVersion;
            private readonly ILogger logger;
            private readonly string prefix;
            private readonly string hubName;
            private readonly EventSourcedDurabilityProviderFactory providerFactory;
            private readonly BlobLogger blobLogger;
            private readonly bool fullTracing;

            public LoggerWrapper(ILogger logger, string category, string hubName, EventSourcedDurabilityProviderFactory providerFactory, BlobLogger blobLogger)
            {
                this.logger = logger;
                this.prefix = $"{providerFactory.eventSourcedSettings.WorkerId} [{category}]";
                this.hubName = hubName;
                this.providerFactory = providerFactory;
                this.blobLogger = blobLogger;
                this.fullTracing = this.providerFactory.traceToBlob || this.providerFactory.traceToConsole || this.providerFactory.traceToEtwExtension;
            }

            public IDisposable BeginScope<TState>(TState state)
            {
                return this.logger.BeginScope(state);
            }

            public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel)
            {
                return this.fullTracing || this.logger.IsEnabled(logLevel);
            }

            public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            {
                if (this.IsEnabled(logLevel))
                {
                    this.logger.Log(logLevel, eventId, state, exception, formatter);

                    if (this.providerFactory.traceToEtwExtension)
                    {
                        EtwEventSource.Instance.ExtensionInformationalEvent(
                        this.hubName,
                        EndToEndTraceHelper.LocalAppName,
                        EndToEndTraceHelper.LocalSlotName,
                        string.Empty,
                        string.Empty,
                        $"{logLevel,-11} {this.prefix} {formatter(state, exception)}",
                        ExtensionVersion);
                    }

                    if (this.providerFactory.traceToConsole || this.blobLogger != null)
                    {
                        string formattedString = $"{DateTime.UtcNow:o} {this.prefix}s{(int)logLevel} {formatter(state, exception)}";

                        if (this.providerFactory.traceToConsole)
                        {
                            System.Console.WriteLine(formattedString);
                        }

                        this.blobLogger?.WriteLine(formattedString);
                    }
                }
            }
        }

        private class BlobLogger
        {
            private readonly DateTime starttime;
            private readonly CloudAppendBlob blob;
            private readonly object flushLock = new object();
            private readonly object lineLock = new object();
            private MemoryStream memoryStream;
            private StreamWriter writer;
            private Timer timer;

            public BlobLogger(EventSourcedDurabilityProviderFactory providerFactory)
            {
                this.starttime = DateTime.UtcNow;

                var storageAccount = CloudStorageAccount.Parse(providerFactory.eventSourcedSettings.StorageConnectionString);
                var client = storageAccount.CreateCloudBlobClient();
                var container = client.GetContainerReference("logs");
                container.CreateIfNotExists();
                this.blob = container.GetAppendBlobReference($"{providerFactory.eventSourcedSettings.WorkerId}.{this.starttime:o}.log");
                this.blob.CreateOrReplace();

                this.memoryStream = new MemoryStream();
                this.writer = new StreamWriter(this.memoryStream);

                var interval = 14000 + new Random().Next(1000);
                this.timer = new Timer(this.Flush, null, interval, interval);
            }

            public void WriteLine(string line)
            {
                lock (this.lineLock)
                {
                    this.writer.WriteLine(line);
                }
            }

            public void Flush(object ignored)
            {
                if (Monitor.TryEnter(this.flushLock))
                {
                    try
                    {
                        MemoryStream toSave = null;

                        // grab current buffer and create new one
                        lock (this.lineLock)
                        {
                            this.writer.Flush();
                            if (this.memoryStream.Position > 0)
                            {
                                toSave = this.memoryStream;
                                this.memoryStream = new MemoryStream();
                                this.writer = new StreamWriter(this.memoryStream);
                            }
                        }

                        if (toSave != null)
                        {
                            // save to storage
                            toSave.Seek(0, SeekOrigin.Begin);
                            this.blob.AppendFromStream(toSave);
                            toSave.Dispose();
                        }
                    }
                    finally
                    {
                        Monitor.Exit(this.flushLock);
                    }
                }
            }
        }
    }
}
