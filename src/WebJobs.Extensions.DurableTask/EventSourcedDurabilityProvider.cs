// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.EventSourced;
using Newtonsoft.Json;

namespace Microsoft.Azure.WebJobs.Extensions.DurableTask
{
    internal class EventSourcedDurabilityProvider : DurabilityProvider
    {
        private readonly EventSourcedOrchestrationService serviceClient;
        private readonly string connectionStringName;

        internal EventSourcedDurabilityProvider(EventSourcedOrchestrationService service, string connectionStringName)
            : base("Event Sourced", service, service, connectionStringName)
        {
            this.serviceClient = service;
            this.connectionStringName = connectionStringName;
        }

        public override bool SupportsEntities => true;

        /// <summary>
        /// The app setting containing the Azure Storage connection string.
        /// </summary>
        public override string ConnectionName => this.connectionStringName;

        /// <inheritdoc/>
        public async override Task<string> RetrieveSerializedEntityState(EntityId entityId, JsonSerializerSettings serializerSettings)
        {
            var instanceId = EntityId.GetSchedulerIdFromEntityId(entityId);
            IList<OrchestrationState> stateList = await this.serviceClient.GetOrchestrationStateAsync(instanceId, false);

            OrchestrationState state = stateList?.FirstOrDefault();
            if (state != null
                && state.OrchestrationInstance != null
                && state.Input != null)
            {
                var schedulerState = JsonConvert.DeserializeObject<SchedulerState>(state.Input, serializerSettings);

                if (schedulerState.EntityExists)
                {
                    return schedulerState.EntityState;
                }
            }

            return null;
        }

        /// <inheritdoc/>
        public async override Task<IList<OrchestrationState>> GetOrchestrationStateWithInputsAsync(string instanceId, bool showInput = true)
        {
            var result = new List<OrchestrationState>();
            var state = await this.serviceClient.GetOrchestrationStateAsync(instanceId, null);
            if (state != null)
            {
                result.Add(state);
            }

            return result;
        }
    }
}
