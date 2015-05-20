﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Orleans;

namespace UnitTestGrains
{

    /// <summary>
    /// A simple grain that allows to set two arguments and then multiply them.
    /// </summary>
    public class ErrorGrain : SimpleGrain.SimpleGrain, IErrorGrain
    {
        private int counter = 0;

        public override Task ActivateAsync()
        {
            logger = GetLogger(String.Format("ErrorGrain-{0}-{1}-{2}", base.RuntimeIdentity, base.Identity, base._Data.ActivationId.ToString()));
            logger.Info("Activate..");
            return TaskDone.Done;
        }

        public Task LogMessage(string msg)
        {
           logger.Info(msg);
           return TaskDone.Done;
        }

        public Task SetAError(int a)
        {
            logger.Info("SetAError={0}", a);
            this.State.A = a;
            throw new Exception("SetAError-Exception");
        }

        public Task SetBError(int a)
        {
            throw new Exception("SetBError-Exception");
        }

        public Task<int> GetAxBError()
        {
            throw new Exception("GetAxBError-Exception");
        }

        public Task<int> GetAxBError(int a, int b)
        {
            throw new Exception("GetAxBError(a,b)-Exception");
        }

        public Task LongMethod(int waitTime)
        {
            Thread.Sleep(waitTime);
            return TaskDone.Done;
        }

        public Task LongMethodWithError(int waitTime)
        {
            Thread.Sleep(waitTime);
            throw new Exception("LongMethodWithError");
        }

        public async Task DelayMethod(int waitTime)
        {
            logger.Info("DelayMethod {0}.", counter);
            counter++;
            await Task.Delay(TimeSpan.FromMilliseconds(1)).WithTimeout(TimeSpan.FromMilliseconds(50));
        }

        public Task Dispose()
        {
            logger.Info("Dispose()");
            //RaiseErrorUpdateEvent(2);
            return TaskDone.Done;
        }

        public Task<int> UnobservedErrorImmideate()
        {
            logger.Info("UnobservedErrorImmideate()");

            bool doThrow = true;
            // the grain method returns OK, but leaves some unobserved promise
            AsyncValue<long> promise = AsyncValue<long>.StartNew(() =>
            {
                if (!doThrow)
                    return 0;
                logger.Info("About to throw 1.");
                throw new ArgumentException("ErrorGrain left Immideate Unobserved Error 1.");
            });
            promise.IntentionallyUnobserved();
            promise = null;
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            return Task.FromResult(11);
        }

        public Task<int> UnobservedErrorDelayed()
        {
            logger.Info("UnobservedErrorDelayed()");
            bool doThrow = true;
            // the grain method rturns OK, but leaves some unobserved promise
            AsyncValue<long> promise = AsyncValue<long>.StartNew(() =>
            {
                if (!doThrow)
                    return 0;
                Thread.Sleep(100);
                logger.Info("About to throw 1.5.");
                throw new ArgumentException("ErrorGrain left Delayed Unobserved Error 1.5.");
            });
            promise.IntentionallyUnobserved();
            promise = null;
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            return Task.FromResult(11);
        }

        public Task<int> UnobservedErrorContinuation2()
        {
            logger.Info("UnobservedErrorContinuation2()");
            // the grain method returns OK, but leaves some unobserved promise
            AsyncValue<long> promise = new AsyncValue<long> (25);
            AsyncCompletion cont = promise.ContinueWith(() =>
                {
                    logger.Info("About to throw 2.");
                    throw new ArgumentException("ErrorGrain left ContinueWith Unobserved Error 2.");
                });
            promise = null;
            cont.IntentionallyUnobserved();
            cont = null;
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            return Task.FromResult(11);
        }

        public Task<int> UnobservedErrorContinuation3()
        {
            logger.Info("UnobservedErrorContinuation3() from Task " + System.Threading.Tasks.Task.CurrentId);
            // the grain method returns OK, but leaves some unobserved promise
            AsyncValue<long> promise = AsyncValue<long>.StartNew(() =>
            {
                logger.Info("First promise from Task " + System.Threading.Tasks.Task.CurrentId);
                return 26;
            });
            AsyncCompletion cont = promise.ContinueWith(() =>
            {
                logger.Info("About to throw 3 from Task " + System.Threading.Tasks.Task.CurrentId);
                throw new ArgumentException("ErrorGrain left ContinueWith Unobserved Error 3.");
            });
            //logger.Info("cont.number=" + cont.task.number + " cont.m_Task.number=" + cont.task.m_Task.Id);
            promise = null;
            cont.IntentionallyUnobserved();
            cont = null;
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            return Task.FromResult(11);
        }

        public Task<int> UnobservedIgnoredError()
        {
            logger.Info("UnobservedIgnoredError()");
            bool doThrow = true;
            // the grain method rturns OK, but leaves some unobserved promise
            AsyncValue<long> promise = AsyncValue<long>.StartNew(() =>
            {
                if (!doThrow)
                    return 0;
                throw new ArgumentException("ErrorGrain left Unobserved Error, but asked to ignore it later.");
            });
            promise.Ignore();
            return Task.FromResult(11);
        }

        public Task AddChildren(List<IErrorGrain> children)
        {
            return TaskDone.Done;
        }

        public async Task<bool> ExecuteDelayed(TimeSpan delay)
        {
            object ctxBefore = AsyncCompletion.Context;

            await Task.Delay(delay);
            object ctxInside = AsyncCompletion.Context;
            return ctxBefore.Equals(ctxInside);
        }
    }
}
