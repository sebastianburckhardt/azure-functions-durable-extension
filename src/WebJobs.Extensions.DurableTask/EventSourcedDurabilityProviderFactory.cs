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

        public EventSourcedDurabilityProviderFactory(
            IOptions<DurableTaskOptions> options,
            IConnectionStringResolver connectionStringResolver,
            ILoggerFactory loggerFactory = null)
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
                this.options.HubName = sanitizedHubName;
            }

            // Use a temporary logger/traceHelper because DurableTaskExtension hasn't been called yet to create one.
            var providerFactoryName = nameof(EventSourcedDurabilityProviderFactory);
            ILogger logger = loggerFactory != null
                    ? loggerFactory.CreateLogger(providerFactoryName)
                    : throw new ArgumentNullException(nameof(loggerFactory));
            var traceHelper = new EndToEndTraceHelper(logger, false);
            traceHelper.ExtensionWarningEvent(this.options.HubName, "n/a", "n/a", $"{providerFactoryName} instantiated");

            var settings = this.GetEventSourcedOrchestrationServiceSettings();

            if (runningInTestEnvironment && cachedTestEntry != null)
            {
                if (settings.Equals(cachedTestEntry.Settings))
                {
                    // We simply use the cached orchestration service, which is still running.
                    this.entry = cachedTestEntry;
                    cachedTestEntry.TaskHubName = this.options.HubName;
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
                DurabilityProvider = new EventSourcedDurabilityProvider(new EventSourcedOrchestrationService(settings), this.defaultConnectionStringName),
                TaskHubName = this.options.HubName,
            };

            if (runningInTestEnvironment)
            {
                if (cachedTestEntry == null)
                {
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

            string connectionName = attribute.ConnectionName ?? this.defaultConnectionStringName;
            var settings = this.GetEventSourcedOrchestrationServiceSettings(connectionName, attribute.TaskHub);

            // It's important that clients use the same EventSourcedOrchestrationService instance
            // as the host when possible to ensure any send operations can be picked up
            // immediately instead of waiting for the next queue polling interval.
            return (string.Equals(this.options.HubName, this.options.HubName, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(this.entry.Settings.StorageConnectionString, settings.StorageConnectionString, StringComparison.OrdinalIgnoreCase))
                ? this.entry.DurabilityProvider
                : new EventSourcedDurabilityProvider(new EventSourcedOrchestrationService(settings), connectionName);
        }

        internal EventSourcedOrchestrationServiceSettings GetEventSourcedOrchestrationServiceSettings(
            string connectionStringName = null,
            string taskHubNameOverride = null)
        {
            if (!string.IsNullOrEmpty(taskHubNameOverride) && !string.Equals(taskHubNameOverride, this.entry.TaskHubName))
            {
                throw new InvalidOperationException("EventSourced client does not support using multiple task hubs");
            }

            string resolvedStorageConnectionString = this.connectionStringResolver.Resolve(connectionStringName ?? this.eventSourcedStorageOptions.ConnectionStringName);
            if (string.IsNullOrEmpty(resolvedStorageConnectionString))
            {
                throw new InvalidOperationException("Unable to find an Azure Storage connection string to use for this binding.");
            }

            string resolvedEventHubsConnectionString = this.connectionStringResolver.Resolve(this.eventSourcedStorageOptions.EventHubsConnectionStringName);
            if (string.IsNullOrEmpty(resolvedEventHubsConnectionString))
            {
                throw new InvalidOperationException("Unable to find an Event Hubs connection string to use for this binding.");
            }

            var settings = new EventSourcedOrchestrationServiceSettings()
            {
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

            public string TaskHubName { get; set; }
        }
    }
}
