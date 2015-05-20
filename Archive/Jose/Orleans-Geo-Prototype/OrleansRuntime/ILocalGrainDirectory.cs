﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Runtime.GrainDirectory;


namespace Orleans.Runtime
{
    interface ILocalGrainDirectory
    {
        /// <summary>
        /// Starts the local portion of the directory service.
        /// </summary>
        void Start();

        /// <summary>
        /// Stops the local portion of the directory service.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1716:IdentifiersShouldNotMatchKeywords", MessageId = "Stop")]
        void Stop(bool doOnStopReplication);

        RemoteGrainDirectory RemGrainDirectory { get; }
        RemoteGrainDirectory CacheValidator { get; }
        Task StopPreparationCompletion { get; }  // Will be resolved when this directory is prepared to stop

        /// <summary>
        /// Registers a new activation with the directory service.
        /// </summary>
        /// <param name="address">The address of the activation to register.</param>
        //void Register(ActivationAddress address);

        /// <summary>
        /// Registers a new activation with the directory service.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="address">The address of the activation to register.</param>
        Task RegisterAsync(ActivationAddress address);

        /// <summary>
        /// Removes the record for an existing activation from the directory service.
        /// This is used when an activation is being deleted.
        /// </summary>
        /// <param name="address">The address of the activation to remove.</param>
        //void Unregister(ActivationAddress address);

        /// <summary>
        /// Removes the record for an existing activation from the directory service.
        /// This is used when an activation is being deleted.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="address">The address of the activation to remove.</param>
        Task UnregisterAsync(ActivationAddress address);

        /// <summary>
        /// Unregister a batch of addresses at once
        /// </summary>
        /// <param name="addresses"></param>
        /// <returns></returns>
        Task UnregisterManyAsync(List<ActivationAddress> addresses);

        /// <summary>
        /// Removes the record for an existing activation from the directory service,
        /// if it was created before the passed-in timestamp.
        /// This is used when a request is received for an activation that cannot be found, 
        /// to lazily clean up the remote directory.
        /// The timestamp is used to prevent removing a valid entry in a possible (but unlikely)
        /// race where a request is received for a new activation before the request that causes the
        /// new activation to be created.
        /// Note that this method is a no-op if the global configuration parameter DirectoryLazyDeregistrationDelay
        /// is a zero or negative TimeSpan.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="address">The address of the activation to remove.</param>
        Task UnregisterConditionallyAsync(ActivationAddress address);

        /// <summary>
        /// Registers a list of new activations with the directory service.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="addresses">The list of the addresses of activations to register.</param>
        //AsyncCompletion RegisterManyAsync(List<ActivationAddress> addresses);

        /// <summary>
        /// Registers a list of new activations with the directory service in single-activation model.
        /// It is assumed that these are all the first activations of their respective grains; 
        /// duplicates are not reported.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="addresses">The list of the addresses of activations to register.</param>
        /// <returns></returns>
        //AsyncCompletion RegisterManySingleActivationAsync(List<ActivationAddress> addresses);

        /// <summary>
        /// Registers a new activation, in single activation mode, with the directory service.
        /// If there is already an activation registered for this grain, then the new activation will
        /// not be registered and the address of the existing activation will be returned.
        /// Otherwise, the passed-in address will be returned.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="address">The address of the potential new activation.</param>
        /// <returns>The address registered for the grain's single activation.</returns>
        Task<ActivationAddress> RegisterSingleActivationAsync(ActivationAddress address);

        /// <summary>
        /// Fetches locally known directory information for a grain.
        /// If there is no local information, either in the cache or in this node's directory partition,
        /// then this method will return false and leave the list empty.
        /// </summary>
        /// <param name="grain">The ID of the grain to look up.</param>
        /// <param name="addresses">An output parameter that receives the list of locally-known activations of the grain.</param>
        /// <returns>True if remote addresses are complete within freshness constraint</returns>
        bool LocalLookup(GrainId grain, out List<ActivationAddress> addresses);

        /// <summary>
        /// Fetches complete directory information for a grain.
        /// If there is no local information, then this method will query the appropriate remote directory node.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="grain">The ID of the grain to look up.</param>
        /// <returns>A list of all known activations of the grain.</returns>
        Task<List<ActivationAddress>> FullLookup(GrainId grain);

        /// <summary>
        /// Removes all directory information about a grain.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="grain">The ID of the grain to look up.</param>
        /// <returns>An acknowledgement that the deletion has completed.
        /// It is safe to ignore this result.</returns>
        Task DeleteGrain(GrainId grain);

        /// <summary>
        /// Invalidates cache entry for the given grain.
        /// This method is intended to be called whenever a directory client tries to access 
        /// an activation returned from the previous directory lookup and gets a reject from the target silo 
        /// notifiying him that the activation does not exist.
        /// </summary>
        /// <param name="grain">The ID of the grain that its entry in the directory cache needs to be invalidated</param>
        void InvalidateCacheEntry(GrainId grain);

        /// <summary>
        /// Partly Invalidates cache entry for the given grain.
        /// </summary>
        /// <param name="grain">The ID of the grain that its entry in the directory cache needs to be partly invalidated.</param>
        /// <param name="activation">The ID of the activation that needs to be invalidated in the directory cache for the given grain.</param>
        void InvalidateCacheEntryPartly(GrainId grain, ActivationId activation);

        /// <summary>
        /// Seeds the cache with the given activation.
        /// </summary>
        /// <param name="entries">The activations to seed the cache with.</param>
        //void AddCacheEntries(IEnumerable<ActivationAddress> entries);

        /// <summary>
        /// Seeds the cache with the given activation.
        /// </summary>
        /// <param name="entries">The activation to seed the cache with.</param>
        //void AddCacheEntry(ActivationAddress activationAddress);

        /// <summary>
        /// For testing purposes only.
        /// Returns the silo that this silo thinks is the primary owner of directory information for
        /// the provided grain ID.
        /// </summary>
        /// <param name="grain"></param>
        /// <returns></returns>
        SiloAddress GetPrimaryForGrain(GrainId grain);

        /// <summary>
        /// For testing purposes only.
        /// Returns the silos that this silo thinks hold replicated directory information for
        /// the provided grain ID.
        /// </summary>
        /// <param name="grain"></param>
        /// <returns></returns>
        List<SiloAddress> GetReplicasForGrain(GrainId grain);

        /// <summary>
        /// For testing purposes only.
        /// Returns the directory information held by a replica silo for the provided grain ID.
        /// The result will be null if no information is held.
        /// </summary>
        /// <param name="grain"></param>
        /// <param name="isPrimary"></param>
        /// <returns></returns>
        List<ActivationAddress> GetLocalDataForGrain(GrainId grain, out bool isPrimary);

        /// <summary>
        /// For testing and troubleshhoting purposes only.
        /// Returns the directory information held in a local for the provided grain ID.
        /// The result will be null if no information is held.
        /// </summary>
        /// <param name="grain"></param>
        /// <returns></returns>
        List<ActivationAddress> GetLocalDirectoryData(GrainId grain);

        /// <summary>
        /// For testing and troubleshhoting purposes only.
        /// Returns the directory information held in a local directory cacche for the provided grain ID.
        /// The result will be null if no information is held.
        /// </summary>
        /// <param name="grain"></param>
        /// <returns></returns>
        List<ActivationAddress> GetLocalCacheData(GrainId grain);

    }
}
