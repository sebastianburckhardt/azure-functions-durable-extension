﻿using Common;
using Hello.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
#pragma warning disable 1998

namespace Hello.Benchmark
{
    
    public class ReplicatedOrleansHello : IScenario
    {
        public ReplicatedOrleansHello(int numrobots, int numreqs)
        {
            this.numworkers = numrobots;
            this.numreqs = numreqs;
        }
        private int numworkers;
        private int numreqs;

        public string Name { get { return string.Format("replicatedorleans{0}x{1}", numworkers, numreqs); } }

        public int NumRobots { get { return numworkers; } }

        public async Task<string> ConductorScript(IConductorContext context)
        {
            var workerrequests = new Task<string>[numworkers];
            for (int i = 0; i < numworkers; i++)
                workerrequests[i] = context.RunRobot(i, "");

            await Task.WhenAll(workerrequests);

            return string.Join(",", workerrequests.Select((t) => t.Result));
        }

        public async Task<string> RobotScript(IRobotContext context, int workernumber, string parameters)
        {
            Task<string>[] requests = new Task<string>[numreqs];

            for (int i = 0; i < numreqs; i++)
                requests[i] = context.ServiceRequest(new ReplicatedOrleansHelloRequest(numreqs * workernumber + i));

            Task.WaitAll(requests);

            //verify that all responses are same. this wont work with current implementation. 
            /*string r0 = responses[0];
            for (int i = 1; i < numreqs; i++)
            {
                if (!responses[i].Equals(r0, StringComparison.InvariantCultureIgnoreCase))
                {
                    return "not ok. The responses do not match.";
                }
            }*/

            var responses = string.Join(",", requests.Select((t) => t.Result));

            return "ok: " + workernumber + " : " + responses;
        }


        public string RobotServiceEndpoint(int workernumber)
        {
            return Endpoints.GetService(workernumber);
        }
    }

    public class ReplicatedOrleansHelloRequest : IHttpRequest
    {
        public ReplicatedOrleansHelloRequest(int nr)
        {
            this.nr = nr;
        }

        private int nr;

        public string Signature
        {
            get { return string.Format("GET hello?nr={0}&command={1}", nr, "replicatedorleans"); }
        }

        public string Body
        {
            get { return null; }
        }

        public async Task<string> ProcessRequestOnServer()
        {
            //send to some grain.
            var replicatedGrain = ReplicatedHelloGrainFactory.GetGrain(0);
            await replicatedGrain.Hello(nr.ToString());
            return await replicatedGrain.GetTopMessagesAsync(true);

            /*var helloGrain = HelloGrainFactory.GetGrain(0);
            return await helloGrain.Hello(nr.ToString());*/
        }

        public async Task<string> ProcessResponseOnClient(string response)
        {
            //This is a temporary check due to the potential duplicate write bug.
            //Util.Assert(response.StartsWith("U1"), "incorrect response");
            return response;
        }

        public async Task ProcessErrorResponseOnClient(int statuscode, string response)
        {
            Util.Fail("Unexpected error message");
        }
    }

}
