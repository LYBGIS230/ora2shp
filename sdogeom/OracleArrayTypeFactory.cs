using System;
using Oracle.DataAccess.Types;
using Oracle.DataAccess.Client;

namespace sdogeom
{
    public abstract class OracleArrayTypeFactoryBase<T> : IOracleArrayTypeFactory
    {
        public Array CreateArray(int numElems)
        {
            return new T[numElems];
        }

        public Array CreateStatusArray(int numElems)
        {
            return null;
        }
    }
}