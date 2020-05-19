// Copyright (c) .NET Foundation. All rights reserved.
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
        private readonly EventSourcedStorageOptions eventSourcedStorageOptions;
        private readonly IConnectionStringResolver connectionStringResolver;
        private readonly string defaultConnectionStringName;
        private readonly ILoggerFactory loggerFactory;

        public EventSourcedDurabilityProviderFactory(
            IOptions<DurableTaskOptions> options,
            IConnectionStringResolver connectionStringResolver,
            ILoggerFactory loggerFactory)
        {
            this.options = options.Value;
            this.eventSourcedStorageOptions = new EventSourcedStorageOptions();
            JsonConvert.PopulateObject(JsonConvert.SerializeObject(this.options.StorageProvider), this.eventSourcedStorageOptions);

            this.eventSourcedStorageOptions.Validate();
            var runningInTestEnvironment = this.eventSourcedStorageOptions.RunningInTestEnvironment;
            this.connectionStringResolver = connectionStringResolver;
            this.defaultConnectionStringName = this.eventSourcedStorageOptions.ConnectionStringName ?? ConnectionStringNames.Storage;

            if (!AzureStorageOptions.IsSanitizedHubName(this.options.HubName, out string sanitizedHubName))
            {
                this.options.SetDefaultHubName(sanitizedHubName);
            }

            if (runningInTestEnvironment)
            {
                // use a single task hub name for all tests to allow reuse between tests with same settings
                this.options.HubName = "test-taskhub";
            }

            // Use a temporary logger/traceHelper because DurableTaskExtension hasn't been called yet to create one.
            this.loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            var providerFactoryName = nameof(EventSourcedDurabilityProviderFactory);
            ILogger logger = this.loggerFactory.CreateLogger(providerFactoryName);
            var traceHelper = new EndToEndTraceHelper(logger, false);
            traceHelper.ExtensionWarningEvent(this.options.HubName, string.Empty, string.Empty, $"{providerFactoryName} instantiated");

            // capture trace events generated in the backend and generate an ETW event
            // this is a temporary workaround until the original ETW events are being captured by the hosted infrastructure
            this.loggerFactory = new LoggerFactoryWrapper(loggerFactory, this.options.HubName);

            var settings = this.GetEventSourcedOrchestrationServiceSettings();

            if (runningInTestEnvironment && cachedTestEntry != null)
            {
                if (settings.Equals(cachedTestEntry.Settings))
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
                Settings = settings,
                DurabilityProvider = new EventSourcedDurabilityProvider(new EventSourcedOrchestrationService(settings, this.loggerFactory), this.defaultConnectionStringName),
            };

            if (runningInTestEnvironment)
            {
                if (cachedTestEntry == null)
                {
                    // delete the test taskhub before the first test is run
                    ((IOrchestrationService)this.entry.DurabilityProvider).DeleteAsync().Wait();
                }

                cachedTestEntry = this.entry;
            }
        }

        internal string GetDefaultStorageConnectionString() => this.entry.Settings.StorageConnectionString;

        public DurabilityProvider GetDurabilityProvider() => this.entry.DurabilityProvider;

        public DurabilityProvider GetDurabilityProvider(DurableClientAttribute attribute)
        {
            // logger.LogWarning($"{nameof(EventSourcedDurabilityProviderFactory)}.{nameof(this.GetDurabilityProvider)}");

            this.eventSourcedStorageOptions.ValidateHubName(this.options.HubName);

            string connectionName = attribute.ConnectionName ?? this.defaultConnectionStringName;
            var settings = this.GetEventSourcedOrchestrationServiceSettings(connectionName, attribute.TaskHub);

            // By design, we are only running one instance of the service on each host machine, for each task hub
            return (string.Equals(this.options.HubName, this.options.HubName, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(this.entry.Settings.StorageConnectionString, settings.StorageConnectionString, StringComparison.OrdinalIgnoreCase))
                ? this.entry.DurabilityProvider
                : new EventSourcedDurabilityProvider(new EventSourcedOrchestrationService(settings, this.loggerFactory), connectionName);
        }

        internal EventSourcedOrchestrationServiceSettings GetEventSourcedOrchestrationServiceSettings(
            string connectionStringName = null,
            string taskHubNameOverride = null)
        {
            string resolvedStorageConnectionString = this.connectionStringResolver.Resolve(connectionStringName ?? this.eventSourcedStorageOptions.ConnectionStringName);
            if (string.IsNullOrEmpty(resolvedStorageConnectionString))
            {
                throw new InvalidOperationException($"Unable to resolve configuration variable ${this.eventSourcedStorageOptions.EventHubsConnectionStringName} for the Azure storage connection string.");
            }

            string resolvedEventHubsConnectionString = this.connectionStringResolver.Resolve(this.eventSourcedStorageOptions.EventHubsConnectionStringName);
            if (string.IsNullOrEmpty(resolvedEventHubsConnectionString))
            {
                throw new InvalidOperationException($"Unable to resolve configuration variable ${this.eventSourcedStorageOptions.EventHubsConnectionStringName} for the EventHubs connection string.");
            }

            var settings = new EventSourcedOrchestrationServiceSettings()
            {
                TaskHubName = taskHubNameOverride ?? this.options.HubName,
                StorageConnectionString = resolvedStorageConnectionString,
                EventHubsConnectionString = resolvedEventHubsConnectionString,
                MaxConcurrentTaskActivityWorkItems = this.options.MaxConcurrentActivityFunctions,
                MaxConcurrentTaskOrchestrationWorkItems = this.options.MaxConcurrentOrchestratorFunctions,
                KeepServiceRunning = this.eventSourcedStorageOptions.RunningInTestEnvironment,
            };

            return settings;
        }

        private class Entry
        {
            public EventSourcedOrchestrationServiceSettings Settings { get; set; }

            public EventSourcedDurabilityProvider DurabilityProvider { get; set; }
        }

        private class LoggerFactoryWrapper : ILoggerFactory
        {
            private readonly ILoggerFactory loggerFactory;
            private readonly string hubName;

            public LoggerFactoryWrapper(ILoggerFactory loggerFactory, string hubName)
            {
                this.hubName = hubName;
                this.loggerFactory = loggerFactory;
            }

            public void AddProvider(ILoggerProvider provider)
            {
                this.loggerFactory.AddProvider(provider);
            }

            public ILogger CreateLogger(string categoryName)
            {
                var logger = this.loggerFactory.CreateLogger(categoryName);
                return new LoggerWrapper(logger, categoryName, this.hubName);
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

            public LoggerWrapper(ILogger logger, string category, string hubName)
            {
                this.logger = logger;
                this.prefix = $"[{category}]";
                this.hubName = hubName;
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

                    if (EventSourcedDurabilityProvider.GenerateEtwExtensionEventsForILogger)
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
                }
            }
        }
    }
}
