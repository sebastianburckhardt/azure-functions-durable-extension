﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GeoOrleans.Benchmarks.LeaderboardPileus.Interfaces
{


    [Serializable]
    public class Score : IEquatable<Score>
    {
        // Player Name
        public string Name;
        // Points
        public long Points;
        public bool Equals(Score s) { return s.Name == Name && s.Points == Points; }
        public override string ToString() { return Name + "-" + Points; }
        // Converts string representation to Score Object
        public static Score fromString(string pScore)
        {
            string[] ss = pScore.Split('-');
            return new Score
            {
                Name = ss[0],
                Points = long.Parse(ss[1])
            };
        }


        /// <summary>
        /// Utility method to print out current post list
        /// </summary>
        /// <param name="s"></param>
        public static string PrintScores(List<Score> pScores)
        {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < pScores.Count(); i++)
            {
                builder.Append(pScores[i]);
                builder.Append("/");
            }
            if (pScores.Count() == 0)
            {
                builder.Append("No scores");
            }
            return builder.ToString();
        }

    }

    /// <summary>
    /// Request types:
    /// 1) GetTop10 (GET)
    /// 2) PostScore (POST)
    /// </summary>
    public enum LeaderboardRequestT
    {
        GET_SYNC,
        POST_SYNC,
        GET_ASYNC,
        POST_ASYNC
    }



}
