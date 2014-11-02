using System;
using System.Collections.Generic;
using System.Data;
using Oracle.DataAccess.Client;
using MapTools;
using sdogeom;

namespace ora2shp
{
    public class ShapeCreator
    {
        public OraShape CreatePointShape(GeoInfo geom_info, ShapeLib.ShapeType shape_type, string pkVal)
        {            
            OraShape shp = new OraShape();
            shp.ShapePK = pkVal.ToString();
            double[] x = new double[1]; x[0] = Convert.ToDouble(geom_info.Geo.Point.X);
            double[] y = new double[1]; y[0] = Convert.ToDouble(geom_info.Geo.Point.X);
            shp.XList = x;
            shp.YList = y;
            if (shape_type == ShapeLib.ShapeType.PointM)
            {
                double[] m = new double[1]; m[0] = Convert.ToDouble(geom_info.Geo.Point.M);
                shp.MList = m;
            }
            shp.nParts = 1;
            shp.nVertices = 1;
            shp.PartType = null;
            shp.ShapeType = shape_type;
            int[] p = new int[1];
            shp.PartStarts = p;
            return shp;
        }

        /// <summary>
        /// Returns a list of coordinates. If index=0 function will return the X coords,
        /// If index= 1 will return the Y coordinates and if index = 2 will return the
        /// Z or M coordinates
        /// </summary>
        /// <param name="geo_info"></param>
        /// <param name="index">0, 1 or 2</param>
        /// <returns></returns>
        protected List<double> getCoordArrayFromGeom(GeoInfo geo_info, int index)
        {
            List<double> lCoord = new List<double>();
            decimal[] coords = geo_info.Geo.OrdinatesArray;
            string strGType = geo_info.Geo.Sdo_Gtype.ToString();
            int step = 0;
            switch (strGType)
            {
                case "2001": //Point
                case "2005": //Point
                case "2002": //Line
                case "2006": //Multi-part Line
                case "2003": //Polygon
                    step = 2;
                    break;
                case "3001": //3D Point
                case "3006": //Mulit-part 3D Line
                case "3003": //3D Polygon
                case "3301": //MPoint
                case "3302": //MPolyline
                case "3002":
                    if (index > 2)
                        throw new Exception(" Invalid coordinate index");
                    step = 3;
                    break;
                default:
                    throw new Exception(strGType + " type shapes are not supported");
            }
            for (int i = index; i < coords.Length; i+=step)
			{
                
                if (i <= coords.Length)
                {
                    lCoord.Add(Convert.ToDouble(coords[i]));
                }
			}
            return lCoord;
        }
        
        /// <summary>
        /// Returns an OraShape object
        /// </summary>
        /// <param name="geom_info"></param>
        /// <param name="shape_type"></param>
        /// <param name="pkVal"></param>
        /// <returns></returns>
        public OraShape getOraShape(GeoInfo geom_info, ShapeLib.ShapeType shape_type, Int32 pkVal)
        {
            //Console.Write("Getting coordinates....\n");
            Dictionary<string, OraShape> allShapes = new Dictionary<string, OraShape>();
            OraShape shp = new OraShape();
            bool isM = false;
            bool isZ = false;
            List<double[]> coordLists = new List<double[]>();
            List<double> XList = new List<double>();
            List<double> YList = new List<double>();
            List<double> WList = new List<double>();
            bool is3D = false;
            List<int> listParts = new List<int>();
            string sqlstring = string.Empty;
            if (geom_info.Geo.Sdo_Gtype.ToString().StartsWith("33"))
                isM=true;
            if (geom_info.Geo.Sdo_Gtype.ToString().StartsWith("3") && !geom_info.Geo.Sdo_Gtype.ToString().StartsWith("33"))
                isZ = true;

            try
            {
                
                string prevID = string.Empty;
                shp = new OraShape();
                shp.ShapePK = pkVal.ToString();
                XList.Clear();
                YList.Clear();
                WList.Clear();

                shp.XList = getCoordArrayFromGeom(geom_info, 0).ToArray();
                shp.YList = getCoordArrayFromGeom(geom_info, 1).ToArray();
                if (isM) shp.MList = getCoordArrayFromGeom(geom_info, 2).ToArray();
                if (isZ) shp.ZList = getCoordArrayFromGeom(geom_info, 2).ToArray();
                shp.nVertices = shp.XList.Length;
                
            }
            catch (Exception ex)
            {
                string s = ex.Message;
            }
            finally
            {
            }

            return shp;
        }

        public ShapePartInfo getShapePartInfo(GeoInfo geo_info)
        {
           Dictionary<string, ShapePartInfo> allElemInfo = new Dictionary<string, ShapePartInfo>();
            ShapePartInfo shpInfo = new ShapePartInfo();
            List<int> listParts = new List<int>();
            string prevID = string.Empty;
            int p = 0; //Part counter
            
            string sqlstring = string.Empty;
            shpInfo = new ShapePartInfo();
            for (int i = 0; i < geo_info.Geo.ElemArray.Length; i += 3)
            {
                listParts.Add(Convert.ToInt32(geo_info.Geo.ElemArray[i])-1);
                p++;
            }
            shpInfo.nParts = p;
            int[] startParts = listParts.ToArray();
            shpInfo.PartStarts = startParts;


            return shpInfo;
        }

    }
}