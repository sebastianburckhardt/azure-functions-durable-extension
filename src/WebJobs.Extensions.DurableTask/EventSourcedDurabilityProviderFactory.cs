﻿// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System;
using DurableTask.Core;
using DurableTask.EventSourced;
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
        private readonly ILoggerFactory loggerFactory;

        // the following are boolean options that can be passed via host.json
        public const string RunningInTestEnvironmentSetting = "RunningInTestEnvironment";
        public const string TraceToConsole = "TraceToConsole";
        public const string TraceToEtwExtension = "TraceToEtwExtension";

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
            JsonConvert.PopulateObject(JsonConvert.SerializeObject(this.options.StorageProvider), this.eventSourcedSettings);

            bool ReadBooleanSetting(string name) => this.options.StorageProvider.TryGetValue(name, out object objValue)
                && objValue is string stringValue && bool.TryParse(stringValue, out bool boolValue) && boolValue;

            this.runningInTestEnvironment = ReadBooleanSetting(RunningInTestEnvironmentSetting);
            this.traceToConsole = ReadBooleanSetting(TraceToConsole);
            this.traceToEtwExtension = ReadBooleanSetting(TraceToEtwExtension);

            // resolve any indirection in the specification of the two connection strings
            this.eventSourcedSettings.StorageConnectionString = this.ResolveIndirection(
                this.eventSourcedSettings.StorageConnectionString,
                nameof(EventSourcedOrchestrationServiceSettings.StorageConnectionString));
            this.eventSourcedSettings.EventHubsConnectionString = this.ResolveIndirection(
                this.eventSourcedSettings.EventHubsConnectionString,
                nameof(EventSourcedOrchestrationServiceSettings.EventHubsConnectionString));

            if (this.runningInTestEnvironment)
            {
                // use a single task hub name for all tests to allow reuse between tests with same settings
                this.options.HubName = "test-taskhub";
            }
            else if (!string.IsNullOrEmpty(this.eventSourcedSettings.TaskHubName))
            {
                // use the taskhubname specified in the settings
                this.options.HubName = this.eventSourcedSettings.TaskHubName;
            }

            // if the taskhubname is not valid, replace it with a default
            if (!AzureStorageOptions.IsSanitizedHubName(this.options.HubName, out string sanitizedHubName))
            {
                this.options.SetDefaultHubName(sanitizedHubName);
            }

            // make sure the settings we pass on have the fields correctly set
            this.eventSourcedSettings.TaskHubName = this.options.HubName;
            if (this.runningInTestEnvironment)
            {
                this.eventSourcedSettings.KeepServiceRunning = true;
            }

            // Use a temporary logger/traceHelper because DurableTaskExtension hasn't been called yet to create one.
            this.loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            var providerFactoryName = nameof(EventSourcedDurabilityProviderFactory);
            ILogger logger = this.loggerFactory.CreateLogger(providerFactoryName);
            var traceHelper = new EndToEndTraceHelper(logger, false);
            traceHelper.ExtensionWarningEvent(this.options.HubName, string.Empty, string.Empty, $"{providerFactoryName} instantiated");

            // capture trace events generated in the backend and generate an ETW event
            // this is a temporary workaround until the original ETW events are being captured by the hosted infrastructure
            this.loggerFactory = new LoggerFactoryWrapper(this.loggerFactory, this.options.HubName, this);

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

            public LoggerFactoryWrapper(ILoggerFactory loggerFactory, string hubName, EventSourcedDurabilityProviderFactory providerFactory)
            {
                this.hubName = hubName;
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
                return new LoggerWrapper(logger, categoryName, this.hubName, this.providerFactory);
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

            public LoggerWrapper(ILogger logger, string category, string hubName, EventSourcedDurabilityProviderFactory providerFactory)
            {
                this.logger = logger;
                this.prefix = $"[{category}]";
                this.hubName = hubName;
                this.providerFactory = providerFactory;
            }

            public IDisposable BeginScope<TState>(TState state)
            {
                return this.logger.BeginScope(state);
            }

            public bool IsEnabled(LogLevel logLevel)
            {
                return this.logger.IsEnabled(logLevel);
            }

            public void Log<TState>(LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            {
                if (this.logger.IsEnabled(logLevel))
                {
                    this.logger.Log(logLevel, eventId, state, exception, formatter);

                    string formattedString = null;

                    if (this.providerFactory.traceToEtwExtension || this.providerFactory.traceToConsole)
                    {
                        formattedString = $"{logLevel,-11} {this.prefix} {formatter(state, exception)}";
                    }

                    if (this.providerFactory.traceToEtwExtension)
                    {
                        EtwEventSource.Instance.ExtensionInformationalEvent(
                        this.hubName,
                        EndToEndTraceHelper.LocalAppName,
                        EndToEndTraceHelper.LocalSlotName,
                        string.Empty,
                        string.Empty,
                        formattedString,
                        ExtensionVersion);
                    }

                    if (this.providerFactory.traceToConsole)
                    {
                        System.Console.WriteLine(formattedString);
                    }
                }
            }
        }
    }
}
