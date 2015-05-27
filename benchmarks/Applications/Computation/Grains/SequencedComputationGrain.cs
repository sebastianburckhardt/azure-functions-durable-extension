﻿using Orleans;
using ReplicatedGrains;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Computation.Interfaces;
using Orleans.Providers;
#pragma warning disable 1998

namespace Computation.Grains
{

    // An implementation of the leaderboard based on sequenced updates.
    // all operations are synchronous

    [StorageProvider(ProviderName = "AzureStore")]
    public class SequencedComputationGrain : SequencedGrain<SequencedComputationGrain.State>, Computation.Interfaces.ISequencedComputationGrain
    {
        [Serializable]
        public new class State
        {

            public byte[] payload { get; set; }

            public State()
            {
                payload = new byte[100];
            } 

        }

        #region Queries

        public async Task<byte[]> ReadApprox(string post)
        {
            
            return (await GetLocalStateAsync()).payload;
        }

        public async Task<byte[]> ReadCurrent(string post)
        {
            return (await GetGlobalStateAsync()).payload;
        }

        #endregion

        #region Updates

        public async Task WriteNow(int pTime)
        {
            await UpdateGloballyAsync(new WriteEvent() { time = pTime });
        }


        public async Task WriteLater(int pTime)
        {
            await UpdateLocallyAsync(new WriteEvent() { time = pTime });
        }

        public override Task OnActivateAsync()
        {
            
            return base.OnActivateAsync();
        }

        [Serializable]
        public class WriteEvent : IAppliesTo<State>
        {
            public int time { get; set; } 
            public void Update(State state)
            {
                var start = DateTime.Now;
                var end = DateTime.Now;

                int i = 0;
                while ((end-start).TotalMilliseconds<time)
                {
                    i++;
                    end = DateTime.Now;
                }


            }
        }

        #endregion


    }
}

