﻿using GeoOrleans.Benchmarks.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
#pragma warning disable 1998

namespace GeoOrleans.Benchmarks.Hello.Benchmark
{

    public class WebsocketHello : IScenario
    {
        public WebsocketHello(int numworkers, int numreqs)
        {
            this.numworkers = numworkers;
            this.numreqs = numreqs;
        }
        public int numworkers;
        public int numreqs;

        public string Name { get { return string.Format("ws{0}x{1}", numworkers, numreqs); } }

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
            await context.ServiceConnection(new SocketRequest(numreqs));

            return "ok";
        }


        public string RobotServiceEndpoint(int workernumber)
        {
            return Endpoints.GetDefaultService();
        }
    }


    public class SocketRequest : ISocketRequest
    {
        public SocketRequest(int numreqs)
        {
            this.numreqs = numreqs;
        }

        private int numreqs;

        // server/client state
        private int count;

        public string Signature
        {
            get { return "WS hello?command=ws&numreqs=" + numreqs; }
        }

        public async Task ProcessConnectionOnServer(ISocket socket)
        {
            GeoOrleans.Runtime.Common.Util.Assert(count == 0);
        }

        public async Task ProcessMessageOnServer(ISocket socket, string message)
        {
            GeoOrleans.Runtime.Common.Util.Assert(message == "Hello #" + count++, "incorrect message from client");
            await socket.Send(message);
        }

        public async Task ProcessCloseOnServer(ISocket socket, string message)
        {
            GeoOrleans.Runtime.Common.Util.Assert(count == numreqs);
            GeoOrleans.Runtime.Common.Util.Assert(message == "completed");
            await socket.Close("ack");
        }

        public async Task<string> ProcessConnectionOnClient(ISocket socket)
        {
            GeoOrleans.Runtime.Common.Util.Assert(count == 0);
            await socket.Send("Hello #" + count);
            return "connected";
        }

        public async Task<string> ProcessMessageOnClient(ISocket socket, string message)
        {
            GeoOrleans.Runtime.Common.Util.Assert(message == "Hello #" + count);
            if (++count < numreqs)
                await socket.Send("Hello #" + count);
            else
                await socket.Close("completed");
            return await Task.FromResult(message);
        }

        public async Task<string> ProcessCloseOnClient(ISocket socket, string message)
        {
            return "error: connection closed by server";
        }
    }

}
