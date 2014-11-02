using System;
using System.Collections.Generic;
using System.Text;
using Oracle.DataAccess.Types;
using sdogeom.UdtBase;

namespace sdogeom
{
    [OracleCustomTypeMappingAttribute("MDSYS.SDO_POINT_TYPE")]
    public class SdoPoint : OracleCustomTypeBase<SdoPoint>
    {
        private decimal? x;

        [OracleObjectMappingAttribute("X")]
        public decimal? X
        {
            get { return x; }
            set { x = value; }
        }

        private decimal? y;

        [OracleObjectMappingAttribute("Y")]
        public decimal? Y
        {
            get { return y; }
            set { y = value; }
        }

        private decimal? z;

        [OracleObjectMappingAttribute("Z")]
        public decimal? Z
        {
            get { return z; }
            set { z = value; }
        }

        private decimal? m;

        [OracleObjectMappingAttribute("M")]
        public decimal? M
        {
            get { return m; }
            set { m = value; }
        }

        public override void MapFromCustomObject()
        {
            SetValue("X", x);
            SetValue("Y", y);
            SetValue("Z", z);
            SetValue("M", m);
        }

        public override void MapToCustomObject()
        {
            X = GetValue<decimal?>("X");
            Y = GetValue<decimal?>("Y");
            Z = GetValue<decimal?>("Z");
            M = GetValue<decimal?>("M");
        }
    }
}
