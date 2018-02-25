using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Rebus.LiteDb
{
    class CollectionNameCorrector
    {
        private readonly Regex _filter = new Regex("([^A-Za-z0-9_]|(^_))");

        public string CorrectName(string inputString)
        {
            return _filter.Replace(inputString, "");
        }
    }
}
