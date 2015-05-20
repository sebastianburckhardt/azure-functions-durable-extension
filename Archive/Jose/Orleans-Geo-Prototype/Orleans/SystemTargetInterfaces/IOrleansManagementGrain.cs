﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Counters;


namespace Orleans.Management
{
    /// <summary>
    /// Interface for system management functions for an Orleans silo, 
    /// exposed as a grain for receiving remote requests / commands.
    /// </summary>
    public interface IOrleansManagementGrain : IGrain
    {
        /// <summary>
        /// Get the list of silo hosts and statuses currently known about in this cluster.
        /// </summary>
        /// <param name="onlyActive">Whether data on just current active silos should be returned, 
        /// or by default data for all current and previous silo instances [including those in Joining or Dead status].</param>
        /// <returns></returns>
        Task<Dictionary<SiloAddress, SiloStatus>> GetHosts(bool onlyActive = false);

        /// <summary>
        /// Set the current log level for system runtime components.
        /// </summary>
        /// <param name="hostsIds">List of silos this command is to be sent to.</param>
        /// <param name="traceLevel">New log level to use.</param>
        /// <returns>Completion promise for this operation.</returns>
        Task SetSystemLogLevel(SiloAddress[] hostsIds, int traceLevel);
        /// <summary>
        /// Set the current log level for application grains.
        /// </summary>
        /// <param name="hostsIds">List of silos this command is to be sent to.</param>
        /// <param name="traceLevel">New log level to use.</param>
        /// <returns>Completion promise for this operation.</returns>
        Task SetAppLogLevel(SiloAddress[] hostsIds, int traceLevel);
        /// <summary>
        /// Set the current log level for a particular Logger, by name (with prefix matching).
        /// </summary>
        /// <param name="hostsIds">List of silos this command is to be sent to.</param>
        /// <param name="logName">Name of the Logger (with prefix matching) to change.</param>
        /// <param name="traceLevel">New log level to use.</param>
        /// <returns>Completion promise for this operation.</returns>
        Task SetLogLevel(SiloAddress[] hostsIds, string logName, int traceLevel);

        /// <summary>
        /// Perform a run of the .NET garbage collector in the specified silos.
        /// </summary>
        /// <param name="hostsIds">List of silos this command is to be sent to.</param>
        /// <returns>Completion promise for this operation.</returns>
        Task ForceGarbageCollection(SiloAddress[] hostsIds);
        /// <summary>
        /// Perform a run of the Orleans activation collecter in the specified silos.
        /// </summary>
        /// <param name="hostsIds">List of silos this command is to be sent to.</param>
        /// <returns>Completion promise for this operation.</returns>
        Task ForceActivationCollection(SiloAddress[] hostsIds, TimeSpan ageLimit);
        Task ForceActivationCollection(TimeSpan ageLimit);
        /// <summary>
        /// Perform a run of the silo statistics collector in the specified silos.
        /// </summary>
        /// <param name="hostsIds">List of silos this command is to be sent to.</param>
        /// <returns>Completion promise for this operation.</returns>
        Task ForceRuntimeStatisticsCollection(SiloAddress[] siloAddresses);

        /// <summary>
        /// Return the most recent silo runtime statistics information for the specified silos.
        /// </summary>
        /// <param name="hostsIds">List of silos this command is to be sent to.</param>
        /// <returns>Completion promise for this operation.</returns>
        Task<SiloRuntimeStatistics[]> GetRuntimeStatistics(SiloAddress[] hostsIds);
        /// <summary>
        /// Return the most recent grain statistics information, amalgomated across silos.
        /// </summary>
        /// <param name="hostsIds">List of silos this command is to be sent to.</param>
        /// <returns>Completion promise for this operation.</returns>
        Task<SimpleGrainStatistic[]> GetSimpleGrainStatistics(SiloAddress[] hostsIds);
        /// <summary>
        /// Return the most recent grain statistics information, amalgomated across all silos.
        /// </summary>
        /// <returns>Completion promise for this operation.</returns>
        Task<SimpleGrainStatistic[]> GetSimpleGrainStatistics();
        /// <summary>
        /// Return the most recent activation count for a specific grain across all silos.
        /// </summary>
        /// <param name="grainReference">Reference to the grain to be queried.</param>
        /// <returns>Completion promise for this operation.</returns>
        // TODO; Should grainReference param be IGrain rather than GrainReference?
        Task<int> GetGrainActivationCount(GrainReference grainReference);

        /// <summary>
        /// Update the configuration information dynamically. Only a subset of configuration information
        /// can be updated - will throw an error (and make no config changes) if you specify attributes
        /// or elements that cannot be changed. The configuration format is XML, in the same format
        /// as the OrleansConfiguration.xml file. The allowed elements and attributes are:
        /// <pre>
        /// &lt;OrleansConfiguration&gt;
        ///     &lt;Globals&gt;
        ///         &lt;Messaging ResponseTimeout=&quot;?&quot;/&gt;
        ///         &lt;Caching CacheSize=&quot;?&quot;/&gt;
        ///         &lt;Activation CollectionInterval=&quot;?&quot; CollectionAmount=&quot;?&quot; CollectionTotalMemoryLimit=&quot;?&quot; CollectionActivationLimit=&quot;?&quot;/&gt;
        ///         &lt;Liveness ProbeTimeout=&quot;?&quot; TableRefreshTimeout=&quot;?&quot; NumMissedProbesLimit=&quot;?&quot;/&gt;
        ///     &lt;/Globals&gt;
        ///     &lt;Defaults&gt;
        ///         &lt;LoadShedding Enabled=&quot;?&quot; LoadLimit=&quot;?&quot;/&gt;
        ///         &lt;Tracing DefaultTraceLevel=&quot;?&quot; PropagateActivityId=&quot;?&quot;&gt;
        ///             &lt;TraceLevelOverride LogPrefix=&quot;?&quot; TraceLevel=&quot;?&quot;/&gt;
        ///         &lt;/Tracing&gt;
        ///     &lt;/Defaults&gt;
        /// &lt;/OrleansConfiguration&gt;
        /// </pre>
        /// </summary>
        /// <param name="hostIds">Silos to update, or null for all silos</param>
        /// <param name="configuration">XML elements and attributes to update</param>
        /// <returns></returns>
        Task UpdateConfiguration(SiloAddress[] hostIds, Dictionary<string, string> configuration, Dictionary<string,string> tracing);

        /// <summary>
        /// Retrieve the current global configuration settings for each specified silo.
        /// </summary>
        /// <param name="hostsIds">Silos to query, or null for all silos</param>
        /// <returns><c>GlobalConfiguration</c> from each requested silo</returns>
        Task<GlobalConfiguration[]> GetGlobalConfiguration(SiloAddress[] hostsIds);

        /// <summary>
        /// Retrieve the current configuration settings for each specified silo.
        /// </summary>
        /// <param name="hostsIds">Silos to query, or null for all silos</param>
        /// <returns><c>NodeConfiguration</c> from each requested silo</returns>
        Task<NodeConfiguration[]> GetNodeConfiguration(SiloAddress[] hostsIds);

        /// <summary>
        /// Retrieve the total number of activations in the cluster.
        /// </summary>
        /// <returns>A quantity of activations.</returns>
        Task<int> GetTotalActivationCount();
    }
}
