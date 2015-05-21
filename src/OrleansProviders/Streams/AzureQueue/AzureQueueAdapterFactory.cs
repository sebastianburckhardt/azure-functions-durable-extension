/*
Project Orleans Cloud Service SDK ver. 1.0
 
Copyright (c) Microsoft Corporation
 
All rights reserved.
 
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the ""Software""), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

using System;
using System.Threading.Tasks;

using Orleans.Streams;
using Orleans.Runtime;
using Orleans.Providers.Streams.Common;

namespace Orleans.Providers.Streams.AzureQueue
{
    public class AzureQueueAdapterFactory : IQueueAdapterFactory
    {
        private const string CACHE_SIZE = "CacheSize";
        private const int DEFAULT_CACHE_SIZE = 4096;
        
        private string deploymentId;
        private string dataConnectionString;
        private string providerName;
        private int cacheSize;
        private HashRingBasedStreamQueueMapper streamQueueMapper;
        private IQueueAdapterCache adapterCache;

        public const string DATA_CONNECTION_STRING = "DataConnectionString";
        public const string DEPLOYMENT_ID = "DeploymentId";
        public const int NUM_QUEUES = 8; // keep as power of 2.
        
        public virtual void Init(IProviderConfiguration config, string providerName, Logger logger)
        {
            if (config == null) throw new ArgumentNullException("config");
            if (!config.Properties.TryGetValue(DATA_CONNECTION_STRING, out dataConnectionString))
                throw new ArgumentException(String.Format("{0} property not set", DATA_CONNECTION_STRING));
            if (!config.Properties.TryGetValue(DEPLOYMENT_ID, out deploymentId))
                throw new ArgumentException(String.Format("{0} property not set", DEPLOYMENT_ID));
            
            string cacheSizeString;
            cacheSize = DEFAULT_CACHE_SIZE;
            if (config.Properties.TryGetValue(CACHE_SIZE, out cacheSizeString))
            {
                if (!int.TryParse(cacheSizeString, out cacheSize))
                    throw new ArgumentException(String.Format("{0} invalid.  Must be int", CACHE_SIZE));
            }
            this.providerName = providerName;
            streamQueueMapper = new HashRingBasedStreamQueueMapper(NUM_QUEUES, providerName);
            adapterCache = new SimpleQueueAdapterCache(this, cacheSize, logger);
        }

        public virtual Task<IQueueAdapter> CreateAdapter()
        {
            var adapter = new AzureQueueAdapter(streamQueueMapper, dataConnectionString, deploymentId, providerName);
            return Task.FromResult<IQueueAdapter>(adapter);
        }

        public virtual IQueueAdapterCache GetQueueAdapterCache()
        {
            return adapterCache;
        }

        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return streamQueueMapper;
        }
    }
}
