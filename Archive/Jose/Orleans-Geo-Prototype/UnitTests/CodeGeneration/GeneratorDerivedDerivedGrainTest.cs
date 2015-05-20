﻿using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using GeneratorTestGrain;

namespace UnitTests
{
    /// <summary>
    /// Summary description for GrainClientTest
    /// </summary>
    [TestClass]
    public class GeneratorDerivedDerivedGrainTest : UnitTestBase
    {
        const int timeout = 10000;
 
        IGeneratorTestDerivedDerivedGrain grain;
        //ResultHandle result;

        public GeneratorDerivedDerivedGrainTest()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        [ClassCleanup()]
        public static void MyClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        private TestContext testContextInstance;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        
        #region Additional test attributes
        //
        // You can use the following additional attributes as you write your tests:
        //
        // Use ClassInitialize to run code before running the first test in the class
        // [ClassInitialize()]
        // public static void MyClassInitialize(TestContext testContext) { }
        //
        // Use ClassCleanup to run code after all tests in a class have run
        // [ClassCleanup()]
        // public static void MyClassCleanup() { }
        //
        // Use TestInitialize to run code before running each test 
        // [TestInitialize()]
        // public void MyTestInitialize() { }
        //
        // Use TestCleanup to run code after each test has run
        // [TestCleanup()]
        // public void MyTestCleanup() { }
        //
        // Use TestInitialize to run code before running each test 
        #endregion

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void GeneratorDerivedDerivedGrainControlFlow()
        {
            grain = GeneratorTestDerivedDerivedGrainFactory.GetGrain(GetRandomGrainId());
            Assert.IsNotNull(grain);

            Task<bool> boolPromise = grain.StringIsNullOrEmpty();
            Assert.IsTrue(boolPromise.Result);

            Task setPromise = grain.StringSet("Begin");
            setPromise.Wait();

            boolPromise = grain.StringIsNullOrEmpty();
            Assert.IsFalse(boolPromise.Result);

            Task<MemberVariables> structPromise = grain.GetMemberVariables();
            Assert.AreEqual("Begin", structPromise.Result.stringVar);
            
            ReplaceArguments arguments = new ReplaceArguments("Begin", "End");
            Task<string> strPromise = grain.StringReplace(arguments);
            Assert.AreEqual("End", strPromise.Result);
            
            strPromise = grain.StringConcat("Begin", "Cont", "End");
            Assert.AreEqual("BeginContEnd", strPromise.Result);
            
            string[] strArray = { "Begin", "Cont", "Cont", "End" };
            strPromise = grain.StringNConcat(strArray);
            Assert.AreEqual("BeginContContEnd", strPromise.Result);
            
            System.Text.ASCIIEncoding encoding = new System.Text.ASCIIEncoding();
            byte[] bytes = encoding.GetBytes("ByteBegin");
            string str = "StringBegin";
            MemberVariables memberVariables = new MemberVariables(bytes, str, ReturnCode.Fail);

            setPromise = grain.SetMemberVariables(memberVariables);
            setPromise.Wait();

            structPromise = grain.GetMemberVariables();
            
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();

            Assert.AreEqual("ByteBegin", enc.GetString(structPromise.Result.byteArray));
            Assert.AreEqual("StringBegin", structPromise.Result.stringVar);
            Assert.AreEqual(ReturnCode.Fail, structPromise.Result.code);
        }
    }
}
