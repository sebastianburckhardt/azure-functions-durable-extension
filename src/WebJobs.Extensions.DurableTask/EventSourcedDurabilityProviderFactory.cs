// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
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
        private static ConcurrentDictionary<DurableClientAttribute, EventSourcedDurabilityProvider> cachedProviders = new ConcurrentDictionary<DurableClientAttribute, EventSourcedDurabilityProvider>();

        private readonly DurableTaskOptions options;
        private readonly IConnectionStringResolver connectionStringResolver;

        private readonly bool reuseTaskHubForAllTests;
        private readonly bool traceToConsole;
        private readonly bool traceToEtwExtension;
        private readonly bool traceToBlob;

        private ILoggerFactory loggerFactory;
        private static BlobLogger blobLogger;
        private EventSourcedDurabilityProvider defaultProvider;

        // the following are boolean options that can be specified in the json,
        // but are not passed on to the backend
        public const string ReuseTaskHubForTests = "ReuseTaskHubForTests";
        public const string TraceToConsole = "TraceToConsole";
        public const string TraceToEtwExtension = "TraceToEtwExtension";
        public const string TraceToBlob = "TraceToBlob";

        public EventSourcedDurabilityProviderFactory(
            IOptions<DurableTaskOptions> options,
            IConnectionStringResolver connectionStringResolver,
            ILoggerFactory loggerFactory)
        {
            // for debugging
            // System.Threading.Thread.Sleep(5000);

            this.options = options.Value;
            this.connectionStringResolver = connectionStringResolver;
            this.loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));

            bool ReadBooleanSetting(string name) => this.options.StorageProvider.TryGetValue(name, out object objValue)
                && objValue is string stringValue && bool.TryParse(stringValue, out bool boolValue) && boolValue;

            this.reuseTaskHubForAllTests = ReadBooleanSetting(ReuseTaskHubForTests);
            this.traceToConsole = ReadBooleanSetting(TraceToConsole);
            this.traceToEtwExtension = ReadBooleanSetting(TraceToEtwExtension);
            this.traceToBlob = ReadBooleanSetting(TraceToBlob);
        }

        private EventSourcedOrchestrationServiceSettings GetEventSourcedSettings(string taskHubNameOverride = null)
        {
            var eventSourcedSettings = new EventSourcedOrchestrationServiceSettings();

            // override DTFx defaults to the defaults we want to use in DF
            eventSourcedSettings.ThrowExceptionOnInvalidDedupeStatus = true;

            // copy all applicable fields from both the options and the storageProvider options
            JsonConvert.PopulateObject(JsonConvert.SerializeObject(this.options), eventSourcedSettings);
            JsonConvert.PopulateObject(JsonConvert.SerializeObject(this.options.StorageProvider), eventSourcedSettings);

            // resolve any indirection in the specification of the two connection strings
            eventSourcedSettings.StorageConnectionString = this.ResolveIndirection(
                eventSourcedSettings.StorageConnectionString,
                nameof(EventSourcedOrchestrationServiceSettings.StorageConnectionString));
            eventSourcedSettings.EventHubsConnectionString = this.ResolveIndirection(
                eventSourcedSettings.EventHubsConnectionString,
                nameof(EventSourcedOrchestrationServiceSettings.EventHubsConnectionString));

            // if worker id is specified in environment, it overrides the configured setting
            string workerId = Environment.GetEnvironmentVariable("WorkerId");
            if (!string.IsNullOrEmpty(workerId))
            {
                eventSourcedSettings.WorkerId = workerId;
            }

            eventSourcedSettings.HubName = this.options.HubName;

            if (taskHubNameOverride != null)
            {
                eventSourcedSettings.HubName = taskHubNameOverride;
            }

            if (this.reuseTaskHubForAllTests)
            {
                eventSourcedSettings.HubName = "test-taskhub";
                eventSourcedSettings.KeepServiceRunning = true;
            }

            // TODO sanitize hubname in the same way as AzureStorage does

            return eventSourcedSettings;
        }

        public void CreateDefaultProvider()
        {
            var settings = this.GetEventSourcedSettings();

            if (this.traceToBlob && blobLogger == null)
            {
                blobLogger = blobLogger ?? new BlobLogger(settings.StorageConnectionString, settings.WorkerId);
            }

            if (this.traceToConsole || this.traceToEtwExtension || this.traceToBlob)
            {
                // capture trace events generated in the backend and redirect them to generate an ETW event, or to trace to console
                this.loggerFactory = new LoggerFactoryWrapper(this.loggerFactory, settings.HubName, settings.WorkerId, this);
            }

            // var providerFactoryName = nameof(EventSourcedDurabilityProviderFactory);
            // ILogger logger = this.loggerFactory.CreateLogger(providerFactoryName);
            // var traceHelper = new EndToEndTraceHelper(logger, false);
            // traceHelper.ExtensionWarningEvent(this.options.HubName, string.Empty, string.Empty, $"{providerFactoryName} instantiated");

            var key = new DurableClientAttribute()
            {
                TaskHub = settings.HubName,
                ConnectionName = settings.StorageConnectionString,
            };

            if (this.reuseTaskHubForAllTests && cachedProviders.TryGetValue(key, out var cachedProviderFromLastTest))
            {
                // We simply use the cached orchestration service, which is still running,
                // but change the extended sessions setting, which is dynamically checked by the implementation.
                cachedProviderFromLastTest.Settings.ExtendedSessionsEnabled = settings.ExtendedSessionsEnabled;
                this.defaultProvider = cachedProviderFromLastTest;
            }
            else
            {
                var service = new EventSourcedOrchestrationService(settings, this.loggerFactory);
                this.defaultProvider = new EventSourcedDurabilityProvider(service, settings);
                cachedProviders[key] = this.defaultProvider;
            }
        }

        public DurabilityProvider GetDurabilityProvider(DurableClientAttribute attribute)
        {
            EventSourcedOrchestrationServiceSettings settings = this.GetEventSourcedSettings(attribute.TaskHub);

            if (string.Equals(this.defaultProvider.Settings.HubName, settings.HubName, StringComparison.OrdinalIgnoreCase) &&
                 string.Equals(this.defaultProvider.Settings.StorageConnectionString, settings.StorageConnectionString, StringComparison.OrdinalIgnoreCase))
            {
                return this.defaultProvider;
            }

            DurableClientAttribute key = new DurableClientAttribute()
            {
                TaskHub = settings.HubName,
                ConnectionName = settings.StorageConnectionString,
            };

            return cachedProviders.GetOrAdd(key, _ =>
            {
                var service = new EventSourcedOrchestrationService(settings, this.loggerFactory);
                return new EventSourcedDurabilityProvider(service, settings);
            });
        }

        public static bool RemoveDurabilityProvider(EventSourcedDurabilityProvider provider)
        {
            return cachedProviders.TryRemove(
                new DurableClientAttribute()
                {
                    TaskHub = provider.Settings.HubName,
                    ConnectionName = provider.Settings.StorageConnectionString,
                },
                out _);
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

        internal string GetDefaultStorageConnectionString()
            => this.defaultProvider.Settings.StorageConnectionString;

        public DurabilityProvider GetDurabilityProvider()
        {
            if (this.defaultProvider == null)
            {
                this.CreateDefaultProvider();
            }

            return this.defaultProvider;
        }

        private class LoggerFactoryWrapper : ILoggerFactory
        {
            private readonly ILoggerFactory loggerFactory;
            private readonly EventSourcedDurabilityProviderFactory providerFactory;
            private readonly string hubName;
            private readonly string workerId;

            public LoggerFactoryWrapper(ILoggerFactory loggerFactory, string hubName, string workerId, EventSourcedDurabilityProviderFactory providerFactory)
            {
                this.hubName = hubName;
                this.workerId = workerId;
                this.loggerFactory = loggerFactory;
                this.providerFactory = providerFactory;
            }

            public void AddProvider(ILoggerProvider provider)
            {
                this.loggerFactory.AddProvider(provider);
            }

            public ILogger CreateLogger(string categoryName)
            {
                var logger = this.loggerFactory.CreateLogger(categoryName);
                return new LoggerWrapper(logger, categoryName, this.hubName, this.workerId, this.providerFactory);
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
            private readonly bool fullTracing;

            public LoggerWrapper(ILogger logger, string category, string hubName, string workerId, EventSourcedDurabilityProviderFactory providerFactory)
            {
                this.logger = logger;
                this.prefix = $"{workerId} [{category}]";
                this.hubName = hubName;
                this.providerFactory = providerFactory;
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

                    if (this.providerFactory.traceToConsole || this.providerFactory.traceToBlob)
                    {
                        string formattedString = $"{DateTime.UtcNow:o} {this.prefix}s{(int)logLevel} {formatter(state, exception)}";

                        if (this.providerFactory.traceToConsole)
                        {
                            System.Console.WriteLine(formattedString);
                        }

                        EventSourcedDurabilityProviderFactory.blobLogger?.WriteLine(formattedString);
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
            private readonly Timer timer;
            private MemoryStream memoryStream;
            private StreamWriter writer;

            public BlobLogger(string storageConnectionString, string workerId)
            {
                this.starttime = DateTime.UtcNow;

                var storageAccount = CloudStorageAccount.Parse(storageConnectionString);
                var client = storageAccount.CreateCloudBlobClient();
                var container = client.GetContainerReference("logs");
                container.CreateIfNotExists();
                this.blob = container.GetAppendBlobReference($"{workerId}.{this.starttime:o}.log");
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
