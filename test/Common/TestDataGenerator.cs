﻿// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Azure.WebJobs.Extensions.DurableTask.Tests
{
    public class TestDataGenerator
    {
        private static readonly object[] BoolOptions = new object[] { true, false };
#if !FUNCTIONS_V1
        private static readonly object[] ExtendedSessionsStorageProviders = new object[] { TestHelpers.AzureStorageProviderType, TestHelpers.EventSourcedProviderType };
        private static readonly object[] HistoryStorageProviders = new object[] { TestHelpers.AzureStorageProviderType, TestHelpers.EventSourcedProviderType };
        private static readonly object[] FullFeaturedStorageProviders = new object[] { TestHelpers.AzureStorageProviderType, TestHelpers.EventSourcedProviderType };
        private static readonly object[] RewindStorageProviders = new object[] { TestHelpers.AzureStorageProviderType };
        private static readonly object[] PagedQueriesStorageProviders = new object[] { TestHelpers.AzureStorageProviderType };
        private static readonly object[] AllStorageProviders = new object[] { TestHelpers.AzureStorageProviderType, TestHelpers.RedisProviderType, TestHelpers.EventSourcedProviderType };
#else
        private static readonly object[] ExtendedSessionsStorageProviders = new object[] { TestHelpers.AzureStorageProviderType};
        private static readonly object[] HistoryStorageProviders = new object[] { TestHelpers.AzureStorageProviderType };
        private static readonly object[] FullFeaturedStorageProviders = new object[] { TestHelpers.AzureStorageProviderType };
        private static readonly object[] RewindStorageProviders = new object[] { TestHelpers.AzureStorageProviderType };
        private static readonly object[] PagedQueriesStorageProviders = new object[] { TestHelpers.AzureStorageProviderType };
        private static readonly object[] AllStorageProviders = new object[] { TestHelpers.AzureStorageProviderType }; 
#endif

        public static IEnumerable<object[]> GetFullFeaturedStorageProviderOptions()
        {
            foreach (object storageProviderOption in FullFeaturedStorageProviders)
            {
                yield return new object[] { storageProviderOption };
            }
        }

        public static IEnumerable<object[]> GetBooleanAndFullFeaturedStorageProviderOptions()
        {
            foreach (object boolOption in BoolOptions)
            {
                foreach (object storageProviderOption in FullFeaturedStorageProviders)
                {
                    if (!(bool)boolOption || ExtendedSessionsStorageProviders.Contains(storageProviderOption))
                    {
                        yield return new object[] { boolOption, storageProviderOption };
                    }
                }
            }
        }

        public static IEnumerable<object[]> GetHistoryStorageProviderOptions()
        {
            foreach (object storageProviderOption in HistoryStorageProviders)
            {
                yield return new object[] { storageProviderOption };
            }
        }

        public static IEnumerable<object[]> GetStorageProviderWithRewindOptions()
        {
            foreach (object storageProviderOption in RewindStorageProviders)
            {
                yield return new object[] { storageProviderOption };
            }
        }

        public static IEnumerable<object[]> GetStorageProviderWithPagedQueries()
        {
            foreach (object storageProviderOption in PagedQueriesStorageProviders)
            {
                yield return new object[] { storageProviderOption };
            }
        }

        public static IEnumerable<object[]> GetExtendedSessionAndHistoryStorageProviderOptions()
        {
            foreach (object boolOption in BoolOptions)
            {
                foreach (object storageProviderOption in HistoryStorageProviders)
                {
                    if (!(bool)boolOption || ExtendedSessionsStorageProviders.Contains(storageProviderOption))
                    {
                        yield return new object[] { boolOption, storageProviderOption };
                    }
                }
            }
        }

        public static IEnumerable<object[]> GetAllStorageProviderOptions()
        {
            foreach (object storageProviderOption in AllStorageProviders)
            {
                yield return new object[] { storageProviderOption };
            }
        }

        public static IEnumerable<object[]> GetAllSupportedExtendedSessionWithStorageProviderOptions()
        {
            yield return new object[] { true, TestHelpers.AzureStorageProviderType };
            yield return new object[] { false, TestHelpers.AzureStorageProviderType };

#if !FUNCTIONS_V1
            yield return new object[] { true, TestHelpers.EventSourcedProviderType };
            yield return new object[] { false, TestHelpers.EventSourcedProviderType };
#endif

            // Currently no support for extended sessions in Redis provider type.
            // yield return new object[] { true, TestHelpers.RedisProviderType };

            // Currently no support for Redis provider type in CI. Can uncomment out configuration below if have Redis setup locally.
            // yield return new object[] { false, TestHelpers.RedisProviderType };
        }
    }
}
