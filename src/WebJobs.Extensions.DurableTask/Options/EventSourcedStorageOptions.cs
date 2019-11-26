// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Text;

namespace Microsoft.Azure.WebJobs.Extensions.DurableTask
{
    /// <summary>
    /// Configuration options for the EventSourced storage provider.
    /// </summary>
    public class EventSourcedStorageOptions
    {
        /// <summary>
        /// Gets or sets the name of the Azure Storage connection string used to manage the underlying Azure Storage resources.
        /// </summary>
        /// <remarks>
        /// If not specified, the default behavior is to use the standard `AzureWebJobsStorage` connection string for all storage usage.
        /// </remarks>
        /// <value>The name of a connection string that exists in the app's application settings.</value>
        public string ConnectionStringName { get; set; }

        /// <summary>
        /// Gets or sets the name of the environment variable or configuration setting for the event-sourced backend.
        /// </summary>
        public string EventHubsConnectionStringName { get; set; }

        /// <summary>
        ///  Whether we are running in a test environment. In that case, we reset storage before the first test,
        ///  and we keep the event processor running between tests.
        /// </summary>
        public bool RunningInTestEnvironment { get; set; } = false;

        internal void AddToDebugString(StringBuilder builder)
        {
            builder.Append(nameof(this.ConnectionStringName)).Append(": ").Append(this.ConnectionStringName);
        }

        internal void Validate()
        {
            if (string.IsNullOrEmpty(this.ConnectionStringName))
            {
                throw new InvalidOperationException($"{nameof(this.ConnectionStringName)} must be populated to use the EventSourced storage provider");
            }

            if (string.IsNullOrEmpty(this.EventHubsConnectionStringName))
            {
                throw new InvalidOperationException($"{nameof(this.EventHubsConnectionStringName)} must be populated to use the EventSourced storage provider");
            }
        }
    }
}