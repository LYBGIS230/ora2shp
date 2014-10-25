using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OracleClient;
using MapTools;

namespace ora2shp
{
    public class ShapeCreator
    {
        public OraShape CreateShape(OracleConnection oracon, string table_name, string spatial_col_name, string pk_col_name, string pkvalue, bool pkIsString, ShapeLib.ShapeType shpType)
        {
            if (oracon.State == ConnectionState.Closed)
            { oracon.Open(); }
            OraShape shp = new OraShape();
            OraShape ora_Shape = new OraShape(); //Initialize geometry class
            List<double[]> coordArrays = GetCoordinateArrays(oracon, table_name, spatial_col_name, pk_col_name, pkvalue, pkIsString, shpType);
            ShapePartInfo shpInfo = GetElementInfo(oracon, table_name, spatial_col_name, pk_col_name, pkvalue, pkIsString);

            shp.XList = coordArrays[0];
            shp.YList = coordArrays[1];
            shp.MList = coordArrays[2];
            shp.nParts = shpInfo.nParts;
            shp.nVertices = coordArrays[0].Length;
            shp.PartType = null;
            shp.PartStarts = shpInfo.PartStarts;
            return shp;
        }

        public OraShape CreatePointShape(OracleConnection oracon, string table_name, string spatial_col_name, string pk_col_name, string pkvalue, bool pkIsString, ShapeLib.ShapeType shpType)
        {
            if (oracon.State == ConnectionState.Closed)
            { oracon.Open(); }
            string sqlstring = string.Empty;
            if (!pkIsString)
            {
                if (shpType == ShapeLib.ShapeType.PointM || shpType == ShapeLib.ShapeType.PointZ)
                {
                    sqlstring = @"select round(s." + spatial_col_name + ".SDO_POINT.X,20) as X, " + " round(s." + spatial_col_name + ".SDO_POINT.Y,20) as Y " + " round(s." + spatial_col_name + ".SDO_POINT.Z,20) as W " + " from " + table_name + " s where " + pk_col_name + "=" + pkvalue.ToString();
                }
                else
                {
                    sqlstring = @"select round(s." + spatial_col_name + ".SDO_POINT.X,20) as X, " + " round(s." + spatial_col_name + ".SDO_POINT.Y,20) as Y " + " from " + table_name + " s where " + pk_col_name + "=" + pkvalue.ToString();
                }
            }
            else
            {
                if (shpType == ShapeLib.ShapeType.PointM || shpType == ShapeLib.ShapeType.PointZ)
                {
                    sqlstring = @"select round(s." + spatial_col_name + ".SDO_POINT.X,20) as X, " + " round(s." + spatial_col_name + ".SDO_POINT.Y,20) as Y " + " round(s." + spatial_col_name + ".SDO_POINT.Z,20) as W " + " from " + table_name + " s where " + pk_col_name + "='" + pkvalue.ToString() + "'";
                }
                else
                {
                    sqlstring = @"select round(s." + spatial_col_name + ".SDO_POINT.X,20) as X, " + " round(s." + spatial_col_name + ".SDO_POINT.Y,20) as Y " + " from " + table_name + " s where " + pk_col_name + "='" + pkvalue.ToString() + "'";
                }
            }

            OracleCommand command = oracon.CreateCommand();
            command.CommandText = sqlstring;
            OracleDataReader reader = command.ExecuteReader();
            reader.Read();
            OraShape shp = new OraShape();
            OraShape ora_Shape = new OraShape(); //Initialize geometry class

            double[] x = new double[1]; x[0] = Convert.ToDouble(reader["X"]);
            double[] y = new double[1]; y[0] = Convert.ToDouble(reader["Y"]);
            shp.XList = x;
            shp.YList = y;
            if (shpType == ShapeLib.ShapeType.PointM || shpType == ShapeLib.ShapeType.PointM)
            {
                double[] w = new double[1]; y[0] = Convert.ToDouble(reader["W"]);
                shp.MList = w;
            }
            shp.nParts = 1;
            shp.nVertices = 1;
            shp.PartType = null;
            shp.ShapeType = shpType;
            int[] p = new int[1];
            shp.PartStarts = p;
            reader.Close(); reader.Dispose();
            command.Dispose();
            return shp;
        }

        public ShapePartInfo GetElementInfo(OracleConnection oracon, string table_name, string spatial_col_name, string pk_col_name, string pkvalue, bool pkIsString)
        {
            //Initialize intArray for partStarts
            //Console.Write("Get element info metadata....\n");
            //Console.ReadLine();
            List<int> listParts = new List<int>();
            string sqlstring = string.Empty;
            if (oracon.State == ConnectionState.Closed)
            { oracon.Open(); }
            if (pkIsString)
            {
                sqlstring = @"SELECT COLUMN_VALUE FROM
                                 THE(SELECT CAST(s." + spatial_col_name + @".sdo_elem_info as sdo_geometry.SDO_ELEM_INFO_ARRAY)
                                 FROM " + table_name + " s where " + pk_col_name + "='" + pkvalue + "')";
            }
            else
            {
                sqlstring = @"SELECT COLUMN_VALUE FROM
                                 THE(SELECT CAST(s." + spatial_col_name + @".sdo_elem_info as sdo_geometry.SDO_ELEM_INFO_ARRAY)
                                 FROM " + table_name + " s where " + pk_col_name + "=" + pkvalue + ")";
            }

            OracleCommand command = oracon.CreateCommand();
            command.CommandText = sqlstring;
            OracleDataReader reader = command.ExecuteReader();
            int p = 0; //Part counter
            int i = 0; //Part Start counter
            int r = 1; //Record counter
            while (reader.Read())
            {
                if (i + 1 == r)
                {
                    //Add to intArray
                    listParts.Add(Convert.ToInt32(reader["COLUMN_VALUE"].ToString().Trim()) - 1);
                    p++;
                    i = i + 3;
                }
                r++;
            }

            ShapePartInfo shpInfo = new ShapePartInfo();
            shpInfo.nParts = p;
            int[] startParts = listParts.ToArray();
            shpInfo.PartStarts = startParts;
            reader.Close(); reader.Dispose();
            command.Dispose();
            return shpInfo;
        }

        public Dictionary<string, ShapePartInfo> GetAllElementInfo(OracleConnection oracon, string table_name, string spatial_col_name, string pk_col_name, string where_clause)
        {
            Console.Write("Get element info metadata....\n");
            //Console.ReadLine();
            Dictionary<string, ShapePartInfo> allElemInfo = new Dictionary<string, ShapePartInfo>();
            ShapePartInfo shpInfo = new ShapePartInfo();
            List<int> listParts = new List<int>();
            string prevID = string.Empty;
            int p = 0; //Part counter
            int i = 0; //Part Start counter
            int r = 1; //Record counter

            string sqlstring = string.Empty;
            if (oracon.State == ConnectionState.Closed)
            { oracon.Open(); }

            //Get element info for all shapes
            sqlstring = @"SELECT " + pk_col_name + @", x.*
                           FROM " + table_name + " s, table(s." + spatial_col_name + ".sdo_elem_info) x";

            if (where_clause.Trim() != string.Empty)
            {
                sqlstring = sqlstring + " where " + pk_col_name + " in (select " + pk_col_name + " from " + table_name + " where " + where_clause + ")";
            }

            OracleCommand command = oracon.CreateCommand();
            command.CommandText = sqlstring;
            OracleDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                string curID = reader[0].ToString();
                if (curID != prevID)
                {
                    //New shape. Reinitialise vars.
                    shpInfo = new ShapePartInfo();
                    shpInfo.shpPK = curID;
                    p = 0;
                    i = 0;
                    r = 1;
                    listParts.Clear();
                    prevID = curID;
                    allElemInfo.Add(curID, shpInfo);
                }
                if (i + 1 == r)
                {
                    //Add to intArray
                    listParts.Add(Convert.ToInt32(reader[1].ToString().Trim()) - 1);
                    p++;
                    i = i + 3;
                }
                shpInfo.nParts = p;
                int[] startParts = listParts.ToArray();
                shpInfo.PartStarts = startParts;

                r++;
            }
            reader.Close(); reader.Dispose();
            command.Dispose();
            return allElemInfo;
        }

        protected static List<double[]> GetCoordinateArrays(OracleConnection oracon, string table_name, string spatial_col_name, string pk_col_name, string pkvalue, bool pkIsString, ShapeLib.ShapeType shpType)
        {
            //Console.Write("Getting coordinates....\n");
            //Console.ReadLine();
            //Initialize intArray for partStarts
            List<double[]> coordLists = new List<double[]>();
            List<double> XList = new List<double>();
            List<double> YList = new List<double>();
            List<double> WList = new List<double>();
            bool is3D = false;
            List<int> listParts = new List<int>();
            string sqlstring = string.Empty;
            if (oracon.State == ConnectionState.Closed)
            { oracon.Open(); }
            switch (shpType)
            {
                case ShapeLib.ShapeType.Point:
                case ShapeLib.ShapeType.PolyLine:
                case ShapeLib.ShapeType.Polygon:
                    sqlstring = @"SELECT " + pk_col_name + @" as event_id, round(t.X,20) as X, round(t.Y,20) as Y
                            FROM " + table_name + " c, TABLE(MDSYS.SDO_UTIL.GETVERTICES(c." + spatial_col_name + ")) t ";

                    break;

                case ShapeLib.ShapeType.PointM:
                case ShapeLib.ShapeType.PolyLineM:
                case ShapeLib.ShapeType.PolygonM:
                    sqlstring = @"SELECT " + pk_col_name + @" as event_id, round(t.X,20) as X, round(t.Y,20) as Y, round(t.Z,20) as M
                            FROM " + table_name + " c, TABLE(MDSYS.SDO_UTIL.GETVERTICES(c." + spatial_col_name + ")) t ";
                    is3D = true;
                    break;

                case ShapeLib.ShapeType.PointZ:
                case ShapeLib.ShapeType.PolyLineZ:
                case ShapeLib.ShapeType.PolygonZ:
                    sqlstring = @"SELECT " + pk_col_name + @" as event_id, round(t.X,20) as X, round(t.Y,20) as Y, round(t.W,20) as W
                            FROM " + table_name + " c, TABLE(MDSYS.SDO_UTIL.GETVERTICES(c." + spatial_col_name + ")) t ";
                    is3D = true;
                    break;
            }
            if (pkIsString)
                sqlstring = sqlstring + "WHERE " + pk_col_name + "='" + pkvalue + "'";
            else
                sqlstring = sqlstring + "WHERE " + pk_col_name + "=" + pkvalue;

            OracleCommand command = oracon.CreateCommand();
            command.CommandText = sqlstring;
            try
            {
                OracleDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    XList.Add(Convert.ToDouble(reader[1]));
                    YList.Add(Convert.ToDouble(reader[2]));
                    if (is3D)
                    {
                        WList.Add(Convert.ToDouble(reader[3]));
                    }
                }
                reader.Close();
                reader.Dispose();
            }
            catch (Exception ex)
            {
                string s = ex.Message;
            }
            finally
            {
                command.Dispose();
            }
            coordLists.Add(XList.ToArray());
            coordLists.Add(YList.ToArray());
            coordLists.Add(WList.ToArray());
            return coordLists;
        }

        public Dictionary<string, OraShape> GetAllCoordinateArrays(OracleConnection oracon, string table_name, string spatial_col_name, string pk_col_name, ShapeLib.ShapeType shpType, string where_clause)
        {
            Console.Write("Getting coordinates....\n");
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
            if (oracon.State == ConnectionState.Closed)
            { oracon.Open(); }
            switch (shpType)
            {
                case ShapeLib.ShapeType.Point:
                case ShapeLib.ShapeType.PolyLine:
                case ShapeLib.ShapeType.Polygon:
                    sqlstring = @"SELECT " + pk_col_name + @" as event_id, round(t.X,20) as X, round(t.Y,20) as Y
                            FROM " + table_name + " c, TABLE(MDSYS.SDO_UTIL.GETVERTICES(c." + spatial_col_name + ")) t ";

                    break;

                case ShapeLib.ShapeType.PointM:
                case ShapeLib.ShapeType.PolyLineM:
                case ShapeLib.ShapeType.PolygonM:
                    sqlstring = @"SELECT " + pk_col_name + @" as event_id, round(t.X,20) as X, round(t.Y,20) as Y, round(t.Z,20) as M
                            FROM " + table_name + " c, TABLE(MDSYS.SDO_UTIL.GETVERTICES(c." + spatial_col_name + ")) t ";
                    is3D = true;
                    isM = true;
                    break;

                case ShapeLib.ShapeType.PointZ:
                case ShapeLib.ShapeType.PolyLineZ:
                case ShapeLib.ShapeType.PolygonZ:
                    sqlstring = @"SELECT " + pk_col_name + @" as event_id, round(t.X,20) as X, round(t.Y,20) as Y, round(t.W,20) as W
                            FROM " + table_name + " c, TABLE(MDSYS.SDO_UTIL.GETVERTICES(c." + spatial_col_name + ")) t ";
                    is3D = true;
                    isZ = true;
                    break;
            }

            if (where_clause.Trim() != string.Empty)
            {
                sqlstring = sqlstring + " where " + pk_col_name + " in (select " + pk_col_name + " from " + table_name + " where " + where_clause + ")";
            }

            OracleCommand command = oracon.CreateCommand();
            command.CommandText = sqlstring;
            try
            {
                OracleDataReader reader = command.ExecuteReader();

                string prevID = string.Empty;

                while (reader.Read())
                {
                    string curID = reader[0].ToString();
                    if (curID != prevID)
                    {
                        shp = new OraShape();
                        shp.ShapePK = curID;
                        prevID = curID;
                        XList.Clear();
                        YList.Clear();
                        WList.Clear();
                        allShapes.Add(curID, shp);
                    }
                    XList.Add(Convert.ToDouble(reader[1]));
                    YList.Add(Convert.ToDouble(reader[2]));
                    if (is3D)
                    {
                        WList.Add(Convert.ToDouble(reader[3]));
                    }
                    shp.XList = XList.ToArray();
                    shp.YList = YList.ToArray();
                    if (isM) shp.MList = WList.ToArray();
                    if (isZ) shp.ZList = WList.ToArray();
                    shp.nVertices = shp.XList.Length;
                }
                reader.Close();
                reader.Dispose();
            }
            catch (Exception ex)
            {
                string s = ex.Message;
            }
            finally
            {
                command.Dispose();
            }

            return allShapes;
        }
    }
}