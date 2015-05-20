﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.AzureUtils;
using Orleans.Runtime.MembershipService;

namespace UnitTests.StorageTests
{
    internal static class SiloInstanceTableTestConstants
    {
        internal static string DataConnectionString = "DefaultEndpointsProtocol=https;AccountName=orleanstestdata;AccountKey=qFJFT+YAikJPCE8V5yPlWZWBRGns4oti9tqG6/oYAYFGI4kFAnT91HeiWMa6pddUzDcG5OAmri/gk7owTOQZ+A==";

        internal static readonly TimeSpan Timeout = TimeSpan.FromMinutes(1);

        internal static readonly bool DeleteEntriesAfterTest = true; // false; // Set to false for Debug mode

        internal static readonly string INSTANCE_STATUS_CREATED = SiloStatus.Created.ToString();  //"Created";
        internal static readonly string INSTANCE_STATUS_ACTIVE = SiloStatus.Active.ToString();    //"Active";
        internal static readonly string INSTANCE_STATUS_DEAD = SiloStatus.Dead.ToString();        //"Dead";
    }

    /// <summary>
    /// Tests for operation of Orleans SiloInstanceManager using AzureStore - Requires access to external Azure storage
    /// </summary>
    [TestClass]
    public class SiloInstanceTableManagerTests
    {
        public TestContext TestContext { get; set; }

        private string deploymentId;
        private int generation;
        private SiloAddress siloAddress;
        private SiloInstanceTableEntry myEntry;
        private OrleansSiloInstanceManager manager;
        private readonly Logger logger;

        public SiloInstanceTableManagerTests()
        {
            logger = Logger.GetLogger("SiloInstanceTableManagerTests", Logger.LoggerType.Application);
        }

        // Use ClassInitialize to run code before running the first test in the class
        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            Logger.Initialize(new NodeConfiguration());

            Logger.AddTraceLevelOverride("AzureTableDataManager", OrleansLogger.Severity.Verbose3);
            Logger.AddTraceLevelOverride("OrleansSiloInstanceManager", OrleansLogger.Severity.Verbose3);
        }

        // Use ClassCleanup to run code after all tests in a class have run
        [ClassCleanup]
        public static void ClassCleanup()
        {
            Logger.RemoveTraceLevelOverride("OrleansSiloInstanceManager");
            Logger.RemoveTraceLevelOverride("AzureTableDataManager");

            //Logger.UnInitialize();
        }

        // Use TestInitialize to run code before running each test 
        [TestInitialize]
        public void TestInitialize()
        {
            deploymentId = "test-" + Guid.NewGuid();
            generation = SiloAddress.AllocateNewGeneration();
            siloAddress = SiloAddress.NewLocalAddress(generation);

            logger.Info("DeploymentId={0} Generation={1}", deploymentId, generation);

            logger.Info("Initializing SiloInstanceManager");
            manager = OrleansSiloInstanceManager.GetManager(deploymentId, SiloInstanceTableTestConstants.DataConnectionString)
                .WaitForResultWithThrow(SiloInstanceTableTestConstants.Timeout);
        }

        // Use TestCleanup to run code after each test has run
        [TestCleanup]
        public void TestCleanup()
        {
            if (manager != null && SiloInstanceTableTestConstants.DeleteEntriesAfterTest)
            {
                TimeSpan timeout = SiloInstanceTableTestConstants.Timeout;

                logger.Info("TestCleanup Timeout={0}", timeout);

                logger.Info("TestCleanup -  Finding table entries to cleanup");
                List<Tuple<SiloInstanceTableEntry, string>> entries = 
                    manager.ReadAllTableEntriesForPartitionAsync(deploymentId)
                    .WaitForResultWithThrow(timeout)
                    .ToList();
                logger.Info("TestCleanup -  Found table entries to cleanup {0}", 
                    Utils.IEnumerableToString(entries));

                logger.Info("TestCleanup -  Deleting {0} table entries",
                    entries.Count);
                manager.DeleteTableEntriesAsync(entries)
                    .WaitWithThrow(timeout);

                logger.Info("TestCleanup -  Finished");
                manager = null;
            }
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Azure"), TestCategory("SiloInstanceTable")]
        public void SiloInstanceTable_Op_RegisterSiloInstance()
        {
            RegisterSiloInstance();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Azure"), TestCategory("SiloInstanceTable")]
        public void SiloInstanceTable_Op_ActivateSiloInstance()
        {
            RegisterSiloInstance();

            manager.ActivateSiloInstance(myEntry);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Azure"), TestCategory("SiloInstanceTable")]
        public void SiloInstanceTable_Op_UnregisterSiloInstance()
        {
            RegisterSiloInstance();

            manager.UnregisterSiloInstance(myEntry);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Azure"), TestCategory("SiloInstanceTable")]
        public async Task SiloInstanceTable_Op_InsertSiloEntryConditionally()
        {
            SiloInstanceTableEntry newEntry = manager.CreateTableVersionEntry(0);

            bool didInsert = await manager.InsertSiloEntryConditionally(newEntry, null, null, false)
                .WithTimeout(AzureTableDefaultPolicies.TableOperation_TIMEOUT);

            Assert.IsTrue(didInsert, "Did insert");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Azure"), TestCategory("SiloInstanceTable")]
        public async Task SiloInstanceTable_Register_CheckData()
        {
            const string testName = "SiloInstanceTable_Register_CheckData";
            logger.Info("Start {0}", testName);

            RegisterSiloInstance();

            var data = await FindSiloEntry(siloAddress);
            SiloInstanceTableEntry siloEntry = data.Item1;
            string eTag = data.Item2;

            Assert.IsNotNull(eTag, "ETag should not be null");
            Assert.IsNotNull(siloEntry, "SiloInstanceTableEntry should not be null");

            Assert.AreEqual(SiloInstanceTableTestConstants.INSTANCE_STATUS_CREATED, siloEntry.Status);

            CheckSiloInstanceTableEntry(myEntry, siloEntry);
            logger.Info("End {0}", testName);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Azure"), TestCategory("SiloInstanceTable")]
        public async Task SiloInstanceTable_Activate_CheckData()
        {
            RegisterSiloInstance();

            manager.ActivateSiloInstance(myEntry);

            var data = await FindSiloEntry(siloAddress);
            Assert.IsNotNull(data, "Data returned should not be null");
            
            SiloInstanceTableEntry siloEntry = data.Item1;
            string eTag = data.Item2;

            Assert.IsNotNull(eTag, "ETag should not be null");
            Assert.IsNotNull(siloEntry, "SiloInstanceTableEntry should not be null");

            Assert.AreEqual(SiloInstanceTableTestConstants.INSTANCE_STATUS_ACTIVE, siloEntry.Status);

            CheckSiloInstanceTableEntry(myEntry, siloEntry);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Azure"), TestCategory("SiloInstanceTable")]
        public async Task SiloInstanceTable_Unregister_CheckData()
        {
            RegisterSiloInstance();

            manager.UnregisterSiloInstance(myEntry);

            var data = await FindSiloEntry(siloAddress);
            SiloInstanceTableEntry siloEntry = data.Item1;
            string eTag = data.Item2;

            Assert.IsNotNull(eTag, "ETag should not be null");
            Assert.IsNotNull(siloEntry, "SiloInstanceTableEntry should not be null");

            Assert.AreEqual(SiloInstanceTableTestConstants.INSTANCE_STATUS_DEAD, siloEntry.Status);

            CheckSiloInstanceTableEntry(myEntry, siloEntry);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Azure"), TestCategory("SiloInstanceTable")]
        public void SiloInstanceTable_FindAllGatewayProxyEndpoints()
        {
            RegisterSiloInstance();

            List<IPEndPoint> gateways = manager.FindAllGatewayProxyEndpoints();
            Assert.AreEqual(0, gateways.Count, "Number of gateways before Silo.Activate");

            manager.ActivateSiloInstance(myEntry);

            gateways = manager.FindAllGatewayProxyEndpoints();
            Assert.AreEqual(1, gateways.Count, "Number of gateways after Silo.Activate");

            IPEndPoint myGateway = gateways.First();
            Assert.AreEqual(myEntry.Address, myGateway.Address.ToString(), "Gateway address");
            Assert.AreEqual(myEntry.ProxyPort, myGateway.Port.ToString(CultureInfo.InvariantCulture), "Gateway port");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Azure"), TestCategory("SiloInstanceTable")]
        public void SiloInstanceTable_FindPrimarySiloEndpoint()
        {
            RegisterSiloInstance();

            IPEndPoint primary = manager.FindPrimarySiloEndpoint();
            Assert.IsNull(primary, "Primary silo should not be found before Silo.Activate");

            manager.ActivateSiloInstance(myEntry);

            primary = manager.FindPrimarySiloEndpoint();
            Assert.IsNotNull(primary, "Primary silo should be found after Silo.Activate");

            Assert.AreEqual(myEntry.Address, primary.Address.ToString(), "Primary silo address");
            Assert.AreEqual(myEntry.Port, primary.Port.ToString(CultureInfo.InvariantCulture), "Primary silo port");
        }

        private void RegisterSiloInstance()
        {
            string partitionKey = deploymentId;
            string rowKey = SiloInstanceTableEntry.ConstructRowKey(siloAddress);

            IPEndPoint myEndpoint = siloAddress.Endpoint;

            myEntry = new SiloInstanceTableEntry
            {
                PartitionKey = partitionKey,
                RowKey = rowKey,

                DeploymentId = deploymentId,
                Address = myEndpoint.Address.ToString(),
                Port = myEndpoint.Port.ToString(CultureInfo.InvariantCulture),
                Generation = generation.ToString(CultureInfo.InvariantCulture),

                HostName = myEndpoint.Address.ToString(),
                ProxyPort = "30000",
                Primary = true.ToString(),

                RoleName = "MyRole",
                InstanceName = "MyInstance",
                UpdateZone = "0",
                FaultZone = "0",
                StartTime = Logger.PrintDate(DateTime.UtcNow),
            };

            logger.Info("MyEntry={0}", myEntry);

            manager.RegisterSiloInstance(myEntry);
        }

        private async Task<Tuple<SiloInstanceTableEntry, string>> FindSiloEntry(SiloAddress siloAddr)
        {
            string partitionKey = deploymentId;
            string rowKey = SiloInstanceTableEntry.ConstructRowKey(siloAddr);

            logger.Info("FindSiloEntry for SiloAddress={0} PartitionKey={1} RowKey={2}", siloAddr, partitionKey, rowKey);

            Tuple<SiloInstanceTableEntry, string> data = await manager.ReadSingleTableEntryAsync(partitionKey, rowKey);

            logger.Info("FindSiloEntry returning Data={0}", data);
            return data;
        }

        private void CheckSiloInstanceTableEntry(SiloInstanceTableEntry referenceEntry, SiloInstanceTableEntry entry)
        {
            Assert.AreEqual(referenceEntry.DeploymentId, entry.DeploymentId, "DeploymentId");
            Assert.AreEqual(referenceEntry.Address, entry.Address, "Address");
            Assert.AreEqual(referenceEntry.Port, entry.Port, "Port");
            Assert.AreEqual(referenceEntry.Generation, entry.Generation, "Generation");
            Assert.AreEqual(referenceEntry.HostName, entry.HostName, "HostName");
            //Assert.AreEqual(referenceEntry.Status, entry.Status, "Status");
            Assert.AreEqual(referenceEntry.ProxyPort, entry.ProxyPort, "ProxyPort");
            Assert.AreEqual(referenceEntry.Primary, entry.Primary, "Primary");
            Assert.AreEqual(referenceEntry.RoleName, entry.RoleName, "RoleName");
            Assert.AreEqual(referenceEntry.InstanceName, entry.InstanceName, "InstanceName");
            Assert.AreEqual(referenceEntry.UpdateZone, entry.UpdateZone, "UpdateZone");
            Assert.AreEqual(referenceEntry.FaultZone, entry.FaultZone, "FaultZone");
            Assert.AreEqual(referenceEntry.StartTime, entry.StartTime, "StartTime");
            Assert.AreEqual(referenceEntry.IAmAliveTime, entry.IAmAliveTime, "IAmAliveTime");
            Assert.AreEqual(referenceEntry.MBRVersion, entry.MBRVersion, "MBRVersion");

            Assert.AreEqual(referenceEntry.SuspectingTimes, entry.SuspectingTimes, "SuspectingTimes");
            Assert.AreEqual(referenceEntry.SuspectingSilos, entry.SuspectingSilos, "SuspectingSilos");
        }
    }

    /// <summary>
    /// Tests for operation of Orleans SiloInstanceManager using AzureStore - Requires access to external Azure storage
    /// </summary>
    [TestClass]
    public class AzureMembershipTableTests
    {
        public TestContext TestContext { get; set; }

        private string deploymentId;
        private int generation;
        private SiloAddress siloAddress;
        private AzureBasedMembershipTable membership;
        private static readonly TimeSpan timeout = TimeSpan.FromMinutes(1);
        private readonly Logger logger;

        public AzureMembershipTableTests()
        {
            logger = Logger.GetLogger("AzureMembershipTableTests", Logger.LoggerType.Application);
        }

        // Use ClassInitialize to run code before running the first test in the class
        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            Logger.Initialize(new NodeConfiguration());

            Logger.AddTraceLevelOverride("AzureTableDataManager", OrleansLogger.Severity.Verbose3);
            Logger.AddTraceLevelOverride("OrleansSiloInstanceManager", OrleansLogger.Severity.Verbose3);
            Logger.AddTraceLevelOverride("AzureSiloMembershipTable", OrleansLogger.Severity.Verbose3);
        }

        // Use ClassCleanup to run code after all tests in a class have run
        [ClassCleanup]
        public static void ClassCleanup()
        {
            Logger.RemoveTraceLevelOverride("OrleansSiloInstanceManager");
            Logger.RemoveTraceLevelOverride("AzureSiloMembershipTable");
            Logger.RemoveTraceLevelOverride("AzureTableDataManager");

            //Logger.UnInitialize();
        }

        // Use TestInitialize to run code before running each test 
        [TestInitialize]
        public void TestInitialize()
        {
            deploymentId = "test-" + Guid.NewGuid();
            generation = SiloAddress.AllocateNewGeneration();
            siloAddress = SiloAddress.NewLocalAddress(generation);

            logger.Info("DeploymentId={0} Generation={1}", deploymentId, generation);

            membership = AzureBasedMembershipTable.GetAzureBasedMembershipTable(deploymentId, SiloInstanceTableTestConstants.DataConnectionString, true)
                .WaitForResultWithThrow(timeout);
        }

        // Use TestCleanup to run code after each test has run
        [TestCleanup]
        public void TestCleanup()
        {
            if (membership != null && SiloInstanceTableTestConstants.DeleteEntriesAfterTest)
            {
                membership.DeleteAzureMembershipTableEntries(deploymentId);
                membership = null;
            }
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Azure"), TestCategory("SiloInstanceTable")]
        public async Task AzureMembership_ReadAll_0()
        {
            MembershipTableData data = await membership.ReadAll();
            TableVersion tableVersion = data.Version;
            logger.Info("Membership.ReadAll returned VableVersion={0} Data={1}", tableVersion, data);

            Assert.AreEqual(0, data.Members.Count, "Number of records returned - no table version row");

            string eTag = tableVersion.VersionEtag;
            int ver = tableVersion.Version;

            Assert.IsNotNull(eTag, "ETag should not be null");
            Assert.AreEqual(0, ver, "Initial tabel version should be zero");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Azure"), TestCategory("SiloInstanceTable")]
        public async Task AzureMembership_ReadRow_0()
        {
            MembershipTableData data = await membership.ReadRow(siloAddress);
            TableVersion tableVersion = data.Version;
            logger.Info("Membership.ReadRow returned VableVersion={0} Data={1}", tableVersion, data);

            Assert.AreEqual(0, data.Members.Count, "Number of records returned - no table version row");

            string eTag = tableVersion.VersionEtag;
            int ver = tableVersion.Version;

            logger.Info("Membership.ReadRow returned MembershipEntry ETag={0} TableVersion={1}", eTag, tableVersion);

            Assert.IsNotNull(eTag, "ETag should not be null");
            Assert.AreEqual(0, ver, "Initial tabel version should be zero");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Azure"), TestCategory("SiloInstanceTable")]
        public async Task AzureMembership_ReadRow_1()
        {
            MembershipTableData data = await membership.ReadAll();
            TableVersion tableVersion = data.Version;
            logger.Info("Membership.ReadAll returned VableVersion={0} Data={1}", tableVersion, data);

            Assert.AreEqual(0, data.Members.Count, "Number of records returned - no table version row");

            DateTime now = DateTime.UtcNow;
            MembershipEntry entry = new MembershipEntry
            {
                SiloAddress = siloAddress,
                StartTime = now,
                Status = SiloStatus.Active,
            };

            TableVersion newTableVersion = tableVersion.Next();
            bool ok = await membership.InsertRow(entry, newTableVersion);

            Assert.IsTrue(ok, "InsertRow completed successfully");

            data = await membership.ReadRow(siloAddress);
            tableVersion = data.Version;
            logger.Info("Membership.ReadRow returned VableVersion={0} Data={1}", tableVersion, data);

            Assert.AreEqual(1, data.Members.Count, "Number of records returned - data row only");

            Assert.IsNotNull(tableVersion.VersionEtag, "New version ETag should not be null");
            Assert.AreNotEqual(newTableVersion.VersionEtag, tableVersion.VersionEtag, "New VersionEtag differetnfrom last");
            Assert.AreEqual(newTableVersion.Version, tableVersion.Version, "New table version number");

            MembershipEntry mbrEntry = data.Members[0].Item1;
            string eTag = data.Members[0].Item2;
            logger.Info("Membership.ReadRow returned MembershipEntry ETag={0} Entry={1}", eTag, mbrEntry);

            Assert.IsNotNull(eTag, "ETag should not be null");
            Assert.IsNotNull(mbrEntry, "MembershipEntry should not be null");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Azure"), TestCategory("SiloInstanceTable")]
        public async Task AzureMembership_ReadAll_1()
        {
            MembershipTableData data = await membership.ReadAll();
            TableVersion tableVersion = data.Version;
            logger.Info("Membership.ReadAll returned VableVersion={0} Data={1}", tableVersion, data);

            Assert.AreEqual(0, data.Members.Count, "Number of records returned - no table version row");

            DateTime now = DateTime.UtcNow;
            MembershipEntry entry = new MembershipEntry
            {
                SiloAddress = siloAddress,
                StartTime = now,
                Status = SiloStatus.Active,
            };

            TableVersion newTableVersion = tableVersion.Next();
            bool ok = await membership.InsertRow(entry, newTableVersion);

            Assert.IsTrue(ok, "InsertRow completed successfully");

            data = await membership.ReadAll();
            tableVersion = data.Version;
            logger.Info("Membership.ReadAll returned VableVersion={0} Data={1}", tableVersion, data);

            Assert.AreEqual(1, data.Members.Count, "Number of records returned - data row only");

            Assert.IsNotNull(tableVersion.VersionEtag, "New version ETag should not be null");
            Assert.AreNotEqual(newTableVersion.VersionEtag, tableVersion.VersionEtag, "New VersionEtag differetnfrom last");
            Assert.AreEqual(newTableVersion.Version, tableVersion.Version, "New table version number");

            MembershipEntry mbrEntry = data.Members[0].Item1;
            string eTag = data.Members[0].Item2;
            logger.Info("Membership.ReadAll returned MembershipEntry ETag={0} Entry={1}", eTag, mbrEntry);

            Assert.IsNotNull(eTag, "ETag should not be null");
            Assert.IsNotNull(mbrEntry, "MembershipEntry should not be null");
        }
    }
}
