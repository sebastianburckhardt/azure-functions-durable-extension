// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Text;
using Microsoft.Azure.Storage;

namespace Microsoft.Azure.WebJobs.Extensions.DurableTask
{
    /// <summary>
    /// Configuration options for the EventSourced storage provider.
    /// </summary>
    public class EventSourcedStorageOptions
    {
        // 45 alphanumeric characters gives us a buffer in our table/queue/blob container names.
        private const int MaxTaskHubNameSize = 45;
        private const int MinTaskHubNameSize = 3;

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

        /// <summary>
        /// Throws an exception if the provided hub name violates any naming conventions for the storage provider.
        /// </summary>
        public void ValidateHubName(string hubName)
        {
            try
            {
                NameValidator.ValidateContainerName(hubName.ToLowerInvariant());
            }
            catch (ArgumentException e)
            {
                throw new ArgumentException(GetTaskHubErrorString(hubName), e);
            }

            if (hubName.Length > 50)
            {
                throw new ArgumentException(GetTaskHubErrorString(hubName));
            }
        }

        private static string GetTaskHubErrorString(string hubName)
        {
            return $"Task hub name '{hubName}' should contain only alphanumeric characters and have length between {MinTaskHubNameSize} and {MaxTaskHubNameSize}.";
        }
    }
}