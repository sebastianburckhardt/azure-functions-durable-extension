﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Counters;
using Orleans.Management;


using SimpleGrain;
using UnitTestGrainInterfaces;

// ReSharper disable ConvertToConstant.Local

namespace UnitTests.Management
{
    [TestClass]
    public class ManagementGrainTests : UnitTestBase
    {
        private IOrleansManagementGrain mgmtGrain;

        [TestInitialize]
        public void TestInitialize()
        {
            mgmtGrain = OrleansManagementGrainFactory.GetGrain(RuntimeInterfaceConstants.SystemManagementId);
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Management")]
        public void GetHosts()
        {
            Dictionary<SiloAddress, SiloStatus> siloStatuses = mgmtGrain.GetHosts(true).Result;
            Assert.IsNotNull(siloStatuses, "Got some silo statuses");
            Assert.AreEqual(2, siloStatuses.Count, "Number of silo statuses");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Management")]
        public void GetSiloConfig()
        {
            NodeConfiguration[] nodeConfigs = mgmtGrain.GetNodeConfiguration(null).Result;
            Assert.IsNotNull(nodeConfigs, "Got some silo configs");
            Assert.AreEqual(2, nodeConfigs.Length, "Number of silo configs");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Management")]
        public void GetLoggerConfig()
        {
            ITraceConfiguration[] loggerConfigs = mgmtGrain.GetNodeConfiguration(null).Result;
            Assert.IsNotNull(loggerConfigs, "Got some silo Logger configs");
            Assert.AreEqual(2, loggerConfigs.Length, "Number of silo Logger configs");
        }

        private SimpleGrainStatistic[] GetSimpleGrainStatistics(string when)
        {
            SimpleGrainStatistic[] stats = mgmtGrain.GetSimpleGrainStatistics(null).Result;
            StringBuilder sb = new StringBuilder();
            foreach (var s in stats) sb.Append(s).AppendLine();
            Console.WriteLine("Grain statistics returned by Orleans Management Grain - " + when + " : " + sb);
            return stats;
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Management")]
        public void GetSimpleGrainStatistics()
        {
            SimpleGrainStatistic[] stats = GetSimpleGrainStatistics("Initial");
            Assert.IsTrue(stats.Length > 0, "Got some grain statistics: " + stats.Length);
            foreach (var s in stats)
            {
                Assert.IsFalse(s.GrainType.EndsWith("Activation"), "Grain type name should not end with 'Activation' - " + s.GrainType);
            }
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Management")]
        public void GetGrainStatistics_ActivationCounts_OrleansManagedGrains()
        {
            SimpleGrainStatistic[] stats = GetSimpleGrainStatistics("Before Create");
            Assert.IsTrue(stats.Length > 0, "Got some grain statistics: " + stats.Length);

            string grainType = "SimpleGrain.SimpleGrain";
            Assert.AreEqual(0, stats.Count(s => s.GrainType == grainType), "No activation counter yet for grain: " + grainType);
            ISimpleGrain grain1 = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            int x = grain1.GetA().Result; // Call grain method
            stats = GetSimpleGrainStatistics("After Invoke");
            Assert.AreEqual(1, stats.Count(s => s.GrainType == grainType), "Activation counter now exists for grain: " + grainType);
            SimpleGrainStatistic grainStats = stats.First(s => s.GrainType == grainType);
            Assert.AreEqual(1, grainStats.ActivationCount, "Activation count for grain after activation: " + grainType);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Management")]
        public void GetGrainStatistics_ActivationCounts_SelfManagedGrains()
        {
            SimpleGrainStatistic[] stats = GetSimpleGrainStatistics("Before Create");
            Assert.IsTrue(stats.Length > 0, "Got some grain statistics: " + stats.Length);

            string grainType = "UnitTestGrains.SimpleSelfManagedGrain";
            Assert.AreEqual(0, stats.Count(s => s.GrainType == grainType), "No activation counter yet for grain: " + grainType);
            ISimpleSelfManagedGrain grain1 = SimpleSelfManagedGrainFactory.GetGrain(1);
            long x = grain1.GetKey().Result; // Call grain method
            stats = GetSimpleGrainStatistics("After Invoke");
            Assert.AreEqual(1, stats.Count(s => s.GrainType == grainType), "Activation counter now exists for grain: " + grainType);
            SimpleGrainStatistic grainStats = stats.First(s => s.GrainType == grainType);
            Assert.AreEqual(1, grainStats.ActivationCount, "Activation count for grain after activation: " + grainType);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Management")]
        public void GetSimpleGrainStatistics_ActivationCounts_OrleansManagedGrains()
        {
            SimpleGrainStatistic[] stats = GetSimpleGrainStatistics("Before Create");
            Assert.IsTrue(stats.Length > 0, "Got some grain statistics: " + stats.Length);

            string grainType = "SimpleGrain.SimpleGrain";
            Assert.AreEqual(0, stats.Count(s => s.GrainType == grainType), "No activation counter yet for grain: " + grainType);
            ISimpleGrain grain1 = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            grain1.GetA().Wait(); // Call grain method
            stats = GetSimpleGrainStatistics("After Invoke");
            Assert.AreEqual(1, stats.Count(s => s.GrainType == grainType), "Activation counter now exists for grain: " + grainType);
            SimpleGrainStatistic grainStats = stats.First(s => s.GrainType == grainType);
            Assert.AreEqual(1, grainStats.ActivationCount, "Activation count for grain after activation: " + grainType);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Management")]
        public void GetSimpleGrainStatistics_ActivationCounts_SelfManagedGrains()
        {
            SimpleGrainStatistic[] stats = GetSimpleGrainStatistics("Before Create");
            Assert.IsTrue(stats.Length > 0, "Got some grain statistics: " + stats.Length);

            string grainType = "UnitTestGrains.SimpleSelfManagedGrain";
            Assert.AreEqual(0, stats.Count(s => s.GrainType == grainType), "No activation counter yet for grain: " + grainType);
            ISimpleSelfManagedGrain grain1 = SimpleSelfManagedGrainFactory.GetGrain(2);
            grain1.GetKey().Wait(); // Call grain method
            stats = GetSimpleGrainStatistics("After Invoke");
            Assert.AreEqual(1, stats.Count(s => s.GrainType == grainType), "Activation counter now exists for grain: " + grainType);
            SimpleGrainStatistic grainStats = stats.First(s => s.GrainType == grainType);
            Assert.AreEqual(1, grainStats.ActivationCount, "Activation count for grain after activation: " + grainType);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Management")]
        public void LogLevel_Change()
        {
            ITraceConfiguration[] loggerConfigs = mgmtGrain.GetNodeConfiguration(null).Result;
            Assert.IsNotNull(loggerConfigs, "Got some silo Logger configs");
            Assert.AreEqual(2, loggerConfigs.Length, "Number of silo Logger configs");

            OrleansLogger.Severity logLevel = OrleansLogger.Severity.Info;
            for (int i = 0; i < 2; i++)
            {
                Assert.AreEqual(logLevel, loggerConfigs[i].DefaultTraceLevel, i + " - DefaultTraceLevel initially " + logLevel);
            }

            logLevel = OrleansLogger.Severity.Verbose;
            mgmtGrain.SetSystemLogLevel(null, (int)logLevel).Wait();

            loggerConfigs = mgmtGrain.GetNodeConfiguration(null).Result;
            for (int i = 0; i < 2; i++)
            {
                Assert.AreEqual(logLevel, loggerConfigs[i].DefaultTraceLevel, i + " - DefaultTraceLevel changed to " + logLevel);
            }

            logLevel = OrleansLogger.Severity.Info;
            mgmtGrain.SetSystemLogLevel(null, (int)logLevel).Wait();
            loggerConfigs = mgmtGrain.GetNodeConfiguration(null).Result;
            for (int i = 0; i < 2; i++)
            {
                Assert.AreEqual(logLevel, loggerConfigs[i].DefaultTraceLevel, i + " - DefaultTraceLevel reset to " + logLevel);
            }
        }

        [TestMethod]
        // TODO: [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Management")]
        public void PropagateActivityId_Change()
        {
            const string PropagateActivityIdConfigKey = @"/OrleansConfiguration/Defaults/Tracing/@PropagateActivityId";
            var changeConfig = new Dictionary<string, string>();

            bool propActivityId;
            ITraceConfiguration[] loggerConfigs;

            // ReSharper disable ConditionIsAlwaysTrueOrFalse
            propActivityId = true;
            changeConfig[PropagateActivityIdConfigKey] = propActivityId.ToString();
            Console.WriteLine("Set {0}={1}", PropagateActivityIdConfigKey, changeConfig[PropagateActivityIdConfigKey]);
            mgmtGrain.UpdateConfiguration(null, changeConfig, null).Wait();
            loggerConfigs = mgmtGrain.GetNodeConfiguration(null).Result;
            for (int i = 0; i < 2; i++)
            {
                Assert.AreEqual(propActivityId, loggerConfigs[i].PropagateActivityId, i + " - PropagateActivityId=" + propActivityId);
            }

            propActivityId = false;
            changeConfig[PropagateActivityIdConfigKey] = propActivityId.ToString();
            Console.WriteLine("Set {0}={1}", PropagateActivityIdConfigKey, changeConfig[PropagateActivityIdConfigKey]);
            mgmtGrain.UpdateConfiguration(null, changeConfig, null).Wait();
            loggerConfigs = mgmtGrain.GetNodeConfiguration(null).Result;
            for (int i = 0; i < 2; i++)
            {
                Assert.AreEqual(propActivityId, loggerConfigs[i].PropagateActivityId, i + " - PropagateActivityId=" + propActivityId);
            }

            propActivityId = true;
            changeConfig[PropagateActivityIdConfigKey] = propActivityId.ToString();
            Console.WriteLine("Set {0}={1}", PropagateActivityIdConfigKey, changeConfig[PropagateActivityIdConfigKey]);
            mgmtGrain.UpdateConfiguration(null, changeConfig, null).Wait();
            loggerConfigs = mgmtGrain.GetNodeConfiguration(null).Result;
            for (int i = 0; i < 2; i++)
            {
                Assert.AreEqual(propActivityId, loggerConfigs[i].PropagateActivityId, i + " - PropagateActivityId=" + propActivityId);
            }
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
        }
    }
}

// ReSharper restore ConvertToConstant.Local
