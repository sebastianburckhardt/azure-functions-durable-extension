﻿using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Common;
using Leaderboard.Interfaces; 

#pragma warning disable 1998

namespace Leaderboard.Benchmark
{
    public class Benchmark : IBenchmark
    {
        // name of this benchmark
        public string Name { get { return "leaderboard"; } }

        // list of scenarios for this benchmark
        public IEnumerable<IScenario> Scenarios { get { return scenarios; } }

        private IScenario[] scenarios = new IScenario[] 
        {
            
            /* Robots generate read/write requests in the proportions specified below.
             * Requests are generated in an open-loop and are not currently rate-controlled
             * All robots execute the same load.
             * Staleness bound is set to int.maxValue
             */ 
            
            /* 1. FOR 1 ROBOT */

            /* 1.1 Read-Only Benchmarks */
            // No replication
            new NoReplicationLeaderboard(1, 10000,100),
            // Sequenced Grain. All Global Reads
            new SequencedLeaderboard(1,10000,100,0,0,0),
            // Sequenced Grain, All Local Reads
            new SequencedLeaderboard(1,10000,0,100,0,0),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedLeaderboard(1,10000,75,25,0,0),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedLeaderboard(1,10000,50,50,0,0),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedLeaderboard(1,10000,25,75,0,0),
        
            /* 1.2 Write-Only Benchmarks */
            // No replication
            new NoReplicationLeaderboard(1, 10000,0),
            // Sequenced Grain. All Global writes
            new SequencedLeaderboard(1,10000,0,0,100,0),
            // Sequenced Grain, All Local writes
            new SequencedLeaderboard(1,10000,0,0,0,100),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedLeaderboard(1,10000,0,0,75,25),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedLeaderboard(1,10000,0,0,50,50),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedLeaderboard(1,10000,0,0,25,75),

            /* 1.3 Read-Write Benchmarks */
            /* 1.3.4 Read mostly (ratio rw: 90/10) */
              // no replication
              new NoReplicationLeaderboard(1, 10000,90),
              // Sequenced Grain. All Global ops
             new SequencedLeaderboard(1,10000,90,0,10,0),
              // Sequenced Grain. All Local ops
             new SequencedLeaderboard(1,10000,0,90,0,10),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedLeaderboard(1,10000,45,45,5,5),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedLeaderboard(1,10000,0,90,10,0),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedLeaderboard(1,10000,90,0,0,10),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedLeaderboard(1,10000,45,45,0,10),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedLeaderboard(1,10000,45,45,10,0),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(1,10000,90,0,5,5),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(1,10000,0,90,5,5),

            /* 1.3.5 Write heavy (ratio rw: 70/30) */
            new NoReplicationLeaderboard(1, 10000,70),
                // Sequenced Grain. All Global ops
             new SequencedLeaderboard(1,10000,70,0,30,0),
              // Sequenced Grain. All Local ops
             new SequencedLeaderboard(1,10000,0,70,0,30),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedLeaderboard(1,10000,35,35,15,15),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedLeaderboard(1,10000,0,70,30,0),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedLeaderboard(1,10000,70,0,0,30),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedLeaderboard(1,10000,35,35,0,30),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedLeaderboard(1,10000,35,35,30,0),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(1,10000,70,0,15,15),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(1,10000,0,70,15,15),


            /* 1.3.6 Read/Write (ratio rw: 50/50) */
            new NoReplicationLeaderboard(1, 10000,50),
                // Sequenced Grain. All Global ops
             new SequencedLeaderboard(1,10000,50,0,50,0),
              // Sequenced Grain. All Local ops
             new SequencedLeaderboard(1,10000,0,50,0,50),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedLeaderboard(1,10000,25,25,25,25),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedLeaderboard(1,10000,0,50,50,0),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedLeaderboard(1,10000,50,0,0,50),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedLeaderboard(1,10000,25,25,0,50),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedLeaderboard(1,10000,25,25,50,0),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(1,10000,50,0,25,25),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(1,10000,0,50,25,25),

           
              /* 2. FOR 50 ROBOT */

            /* 2.1 Read-Only Benchmarks */
            // No replication
            new NoReplicationLeaderboard(50, 10000,100),
            // Sequenced Grain. All Global Reads
            new SequencedLeaderboard(50,10000,100,0,0,0),
            // Sequenced Grain, All Local Reads
            new SequencedLeaderboard(50,10000,0,100,0,0),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedLeaderboard(50,10000,75,25,0,0),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedLeaderboard(50,10000,50,50,0,0),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedLeaderboard(50,10000,25,75,0,0),
        
            /* 2.2 Write-Only Benchmarks */
            // No replication
            new NoReplicationLeaderboard(50, 10000,0),
            // Sequenced Grain. All Global writes
            new SequencedLeaderboard(50,10000,0,0,100,0),
            // Sequenced Grain, All Local writes
            new SequencedLeaderboard(50,10000,0,0,0,100),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedLeaderboard(50,10000,0,0,75,25),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedLeaderboard(50,10000,0,0,50,50),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedLeaderboard(50,10000,0,0,25,75),

            /* 2.3 Read-Write Benchmarks */
            /* 2.3.4 Read mostly (ratio rw: 90/10) */
              // no replication
              new NoReplicationLeaderboard(50, 10000,90),
              // Sequenced Grain. All Global ops
             new SequencedLeaderboard(50,10000,90,0,10,0),
              // Sequenced Grain. All Local ops
             new SequencedLeaderboard(50,10000,0,90,0,10),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedLeaderboard(50,10000,45,45,5,5),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedLeaderboard(50,10000,0,90,10,0),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedLeaderboard(50,10000,90,0,0,10),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedLeaderboard(50,10000,45,45,0,10),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedLeaderboard(50,10000,45,45,10,0),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(50,10000,90,0,5,5),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(50,10000,0,90,5,5),

            /* 2.3.5 Write heavy (ratio rw: 70/30) */
            new NoReplicationLeaderboard(50, 10000,70),
                // Sequenced Grain. All Global ops
             new SequencedLeaderboard(50,10000,70,0,30,0),
              // Sequenced Grain. All Local ops
             new SequencedLeaderboard(50,10000,0,70,0,30),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedLeaderboard(50,10000,35,35,15,15),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedLeaderboard(50,10000,0,70,30,0),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedLeaderboard(50,10000,70,0,0,30),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedLeaderboard(50,10000,35,35,0,30),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedLeaderboard(50,10000,35,35,30,0),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(50,10000,70,0,15,15),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(50,10000,0,70,15,15),


            /* 2.3.6 Read/Write (ratio rw: 50/50) */
            new NoReplicationLeaderboard(50, 10000,50),
                // Sequenced Grain. All Global ops
             new SequencedLeaderboard(50,10000,50,0,50,0),
              // Sequenced Grain. All Local ops
             new SequencedLeaderboard(50,10000,0,50,0,50),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedLeaderboard(50,10000,25,25,25,25),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedLeaderboard(50,10000,0,50,50,0),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedLeaderboard(50,10000,50,0,0,50),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedLeaderboard(50,10000,25,25,0,50),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedLeaderboard(50,10000,25,25,50,0),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(50,10000,50,0,25,25),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(50,10000,0,50,25,25),

                   /* 3. FOR 100 ROBOT */

            /* 3.1 Read-Only Benchmarks */
            // No replication
            new NoReplicationLeaderboard(100, 10000,100),
            // Sequenced Grain. All Global Reads
            new SequencedLeaderboard(100,10000,100,0,0,0),
            // Sequenced Grain, All Local Reads
            new SequencedLeaderboard(100,10000,0,100,0,0),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedLeaderboard(100,10000,75,25,0,0),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedLeaderboard(100,10000,50,50,0,0),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedLeaderboard(100,10000,25,75,0,0),
        
            /* 3.2 Write-Only Benchmarks */
            // No replication
            new NoReplicationLeaderboard(100, 10000,0),
            // Sequenced Grain. All Global writes
            new SequencedLeaderboard(100,10000,0,0,100,0),
            // Sequenced Grain, All Local writes
            new SequencedLeaderboard(100,10000,0,0,0,100),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedLeaderboard(100,10000,0,0,75,25),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedLeaderboard(100,10000,0,0,50,50),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedLeaderboard(100,10000,0,0,25,75),

            /* 3.3 Read-Write Benchmarks */
            /* 3.3.4 Read mostly (ratio rw: 90/10) */
              // no replication
              new NoReplicationLeaderboard(100, 10000,90),
              // Sequenced Grain. All Global ops
             new SequencedLeaderboard(100,10000,90,0,10,0),
              // Sequenced Grain. All Local ops
             new SequencedLeaderboard(100,10000,0,90,0,10),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedLeaderboard(100,10000,45,45,5,5),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedLeaderboard(100,10000,0,90,10,0),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedLeaderboard(100,10000,90,0,0,10),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedLeaderboard(100,10000,45,45,0,10),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedLeaderboard(100,10000,45,45,10,0),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(100,10000,90,0,5,5),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(100,10000,0,90,5,5),

            /* 3.3.5 Write heavy (ratio rw: 70/30) */
            new NoReplicationLeaderboard(100, 10000,70),
                // Sequenced Grain. All Global ops
             new SequencedLeaderboard(100,10000,70,0,30,0),
              // Sequenced Grain. All Local ops
             new SequencedLeaderboard(100,10000,0,70,0,30),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedLeaderboard(100,10000,35,35,15,15),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedLeaderboard(100,10000,0,70,30,0),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedLeaderboard(100,10000,70,0,0,30),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedLeaderboard(100,10000,35,35,0,30),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedLeaderboard(100,10000,35,35,30,0),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(100,10000,70,0,15,15),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(100,10000,0,70,15,15),


            /* 2.3.6 Read/Write (ratio rw: 50/50) */
            new NoReplicationLeaderboard(50, 10000,50),
                // Sequenced Grain. All Global ops
             new SequencedLeaderboard(50,10000,50,0,50,0),
              // Sequenced Grain. All Local ops
             new SequencedLeaderboard(50,10000,0,50,0,50),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedLeaderboard(50,10000,25,25,25,25),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedLeaderboard(50,10000,0,50,50,0),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedLeaderboard(50,10000,50,0,0,50),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedLeaderboard(50,10000,25,25,0,50),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedLeaderboard(50,10000,25,25,50,0),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(50,10000,50,0,25,25),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(50,10000,0,50,25,25),

                         /* 3. FOR 100 ROBOT */

            /* 3.1 Read-Only Benchmarks */
            // No replication
            new NoReplicationLeaderboard(100, 10000,100),
            // Sequenced Grain. All Global Reads
            new SequencedLeaderboard(100,10000,100,0,0,0),
            // Sequenced Grain, All Local Reads
            new SequencedLeaderboard(100,10000,0,100,0,0),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedLeaderboard(100,10000,75,25,0,0),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedLeaderboard(100,10000,50,50,0,0),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedLeaderboard(100,10000,25,75,0,0),
        
            /* 3.2 Write-Only Benchmarks */
            // No replication
            new NoReplicationLeaderboard(100, 10000,0),
            // Sequenced Grain. All Global writes
            new SequencedLeaderboard(100,10000,0,0,100,0),
            // Sequenced Grain, All Local writes
            new SequencedLeaderboard(100,10000,0,0,0,100),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedLeaderboard(100,10000,0,0,75,25),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedLeaderboard(100,10000,0,0,50,50),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedLeaderboard(100,10000,0,0,25,75),

            /* 3.3 Read-Write Benchmarks */
            /* 3.3.4 Read mostly (ratio rw: 90/10) */
              // no replication
              new NoReplicationLeaderboard(100, 10000,90),
              // Sequenced Grain. All Global ops
             new SequencedLeaderboard(100,10000,90,0,10,0),
              // Sequenced Grain. All Local ops
             new SequencedLeaderboard(100,10000,0,90,0,10),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedLeaderboard(100,10000,45,45,5,5),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedLeaderboard(100,10000,0,90,10,0),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedLeaderboard(100,10000,90,0,0,10),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedLeaderboard(100,10000,45,45,0,10),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedLeaderboard(100,10000,45,45,10,0),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(100,10000,90,0,5,5),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(100,10000,0,90,5,5),

            /* 3.3.5 Write heavy (ratio rw: 70/30) */
            new NoReplicationLeaderboard(100, 10000,70),
                // Sequenced Grain. All Global ops
             new SequencedLeaderboard(100,10000,70,0,30,0),
              // Sequenced Grain. All Local ops
             new SequencedLeaderboard(100,10000,0,70,0,30),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedLeaderboard(100,10000,35,35,15,15),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedLeaderboard(100,10000,0,70,30,0),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedLeaderboard(100,10000,70,0,0,30),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedLeaderboard(100,10000,35,35,0,30),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedLeaderboard(100,10000,35,35,30,0),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(100,10000,70,0,15,15),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(100,10000,0,70,15,15),


            /* 2.3.6 Read/Write (ratio rw: 50/50) */
            new NoReplicationLeaderboard(100, 10000,50),
                // Sequenced Grain. All Global ops
             new SequencedLeaderboard(100,10000,50,0,50,0),
              // Sequenced Grain. All Local ops
             new SequencedLeaderboard(100,10000,0,50,0,50),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedLeaderboard(100,10000,25,25,25,25),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedLeaderboard(100,10000,0,50,50,0),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedLeaderboard(100,10000,50,0,0,50),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedLeaderboard(100,10000,25,25,0,50),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedLeaderboard(100,10000,25,25,50,0),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(100,10000,50,0,25,25),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(100,10000,0,50,25,25),

                         /* 4. FOR 500 ROBOT */

            /* 4.1 Read-Only Benchmarks */
            // No replication
            new NoReplicationLeaderboard(500, 10000,100),
            // Sequenced Grain. All Global Reads
            new SequencedLeaderboard(500,10000,100,0,0,0),
            // Sequenced Grain, All Local Reads
            new SequencedLeaderboard(500,10000,0,100,0,0),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedLeaderboard(500,10000,75,25,0,0),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedLeaderboard(500,10000,50,50,0,0),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedLeaderboard(500,10000,25,75,0,0),
        
            /* 4.2 Write-Only Benchmarks */
            // No replication
            new NoReplicationLeaderboard(500, 10000,0),
            // Sequenced Grain. All Global writes
            new SequencedLeaderboard(500,10000,0,0,100,0),
            // Sequenced Grain, All Local writes
            new SequencedLeaderboard(500,10000,0,0,0,100),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedLeaderboard(500,10000,0,0,75,25),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedLeaderboard(500,10000,0,0,50,50),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedLeaderboard(500,10000,0,0,25,75),

            /* 4.3 Read-Write Benchmarks */
            /* 4.3.4 Read mostly (ratio rw: 90/10) */
              // no replication
              new NoReplicationLeaderboard(500, 10000,90),
              // Sequenced Grain. All Global ops
             new SequencedLeaderboard(500,10000,90,0,10,0),
              // Sequenced Grain. All Local ops
             new SequencedLeaderboard(500,10000,0,90,0,10),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedLeaderboard(500,10000,45,45,5,5),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedLeaderboard(500,10000,0,90,10,0),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedLeaderboard(500,10000,90,0,0,10),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedLeaderboard(500,10000,45,45,0,10),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedLeaderboard(500,10000,45,45,10,0),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(500,10000,90,0,5,5),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(500,10000,0,90,5,5),

            /* 4.3.5 Write heavy (ratio rw: 70/30) */
            new NoReplicationLeaderboard(500, 10000,70),
                // Sequenced Grain. All Global ops
             new SequencedLeaderboard(500,10000,70,0,30,0),
              // Sequenced Grain. All Local ops
             new SequencedLeaderboard(500,10000,0,70,0,30),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedLeaderboard(500,10000,35,35,15,15),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedLeaderboard(500,10000,0,70,30,0),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedLeaderboard(500,10000,70,0,0,30),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedLeaderboard(500,10000,35,35,0,30),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedLeaderboard(500,10000,35,35,30,0),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(500,10000,70,0,15,15),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(500,10000,0,70,15,15),


            /* 4.3.6 Read/Write (ratio rw: 50/50) */
            new NoReplicationLeaderboard(500, 10000,50),
                // Sequenced Grain. All Global ops
             new SequencedLeaderboard(500,10000,50,0,50,0),
              // Sequenced Grain. All Local ops
             new SequencedLeaderboard(500,10000,0,50,0,50),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedLeaderboard(500,10000,25,25,25,25),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedLeaderboard(500,10000,0,50,50,0),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedLeaderboard(500,10000,50,0,0,50),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedLeaderboard(500,10000,25,25,0,50),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedLeaderboard(500,10000,25,25,50,0),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(500,10000,50,0,25,25),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedLeaderboard(500,10000,0,50,25,25),
        };

        // parsing of http requests
        public IRequest ParseRequest(string verb, IEnumerable<string> urlpath, NameValueCollection arguments, string body)
        {

            if (verb == "WS" && string.Join("/", urlpath) == "leaderboard")
            {
                throw new NotImplementedException();
                /*LeaderboardRequestT requestType = (LeaderboardRequestT) int.Parse(arguments["reqtype"]) ;
                int numReq =  int.Parse(arguments["numreq"]);
                SocketRequest request=null;
                if (requestType == LeaderboardRequestT.GET) { 
                    // GET type
                    request = new SocketRequest(numReq);
                } else {
                    Util.Assert(requestType == LeaderboardRequestT.POST);
                    //TODO unnecessary conversion to "SCORE" type, keep as string?
                    request = new SocketRequest(numReq, Score.fromString(arguments["score"]));
                }
                return request; */
            }

            if (verb == "GET" && string.Join("/", urlpath) == "leaderboard")
            {

                if (int.Parse(arguments["rep"]) == 0)
                {
                    Console.Write("{0}", arguments);
                    LeaderboardRequestT requestType = (LeaderboardRequestT)int.Parse(arguments["reqtype"]);
                    int numReq = int.Parse(arguments["numreq"]);


                    HttpRequestLeaderboard request = null;
                    if (requestType == LeaderboardRequestT.GET_SYNC)
                    {
                        // GetTop10 type
                        request = new HttpRequestLeaderboard(numReq);
                    }
                    else if (requestType == LeaderboardRequestT.POST_SYNC)
                    {
                        // New score type
                        Util.Assert(requestType == LeaderboardRequestT.POST_SYNC);
                        request = new HttpRequestLeaderboard(numReq, Score.fromString(arguments["score"]));
                    }

                    return request;
                }

                else
                {
                    Console.Write("{0}", arguments);
                    LeaderboardRequestT requestType = (LeaderboardRequestT)int.Parse(arguments["reqtype"]);
                    int numReq = int.Parse(arguments["numreq"]);


                    HttpRequestSequencedLeaderboard request = null;
                    if (requestType == LeaderboardRequestT.GET_SYNC)
                    {
                        // GetCurrentTop10 type
                        request = new HttpRequestSequencedLeaderboard(numReq, false);
                    }
                    else if (requestType == LeaderboardRequestT.GET_ASYNC)
                    {

                        // GetApproxTop10 type
                        request = new HttpRequestSequencedLeaderboard(numReq, true);

                    }
                    else if (requestType == LeaderboardRequestT.POST_SYNC)
                    {
                        // Post Now Type
                        request = new HttpRequestSequencedLeaderboard(numReq, Score.fromString(arguments["score"]), false);
                    }
                    else if (requestType == LeaderboardRequestT.POST_ASYNC)
                    {
                        // Post Later Type
                        request = new HttpRequestSequencedLeaderboard(numReq, Score.fromString(arguments["score"]), true);
                    }

                    return request;
                }
            }

            return null; // URL not recognized
        }

    }


}
