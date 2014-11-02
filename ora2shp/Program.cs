using System;
using System.Collections.Generic;
using System.Data;
using Oracle.DataAccess.Client;
using System.IO;
using MapTools;
using orashpUtils;
using sdogeom;

namespace ora2shp
{
    public class GeoInfo
    {
        private decimal id;
        public decimal Id
        {
            get { return id; }
            set { id = value; }
        }

        private SdoGeometry geo;
        public SdoGeometry Geo
        {
            get { return geo; }
            set { geo = value; }
        }
    }
    internal class Program
    {
        private static DateTime tstart = DateTime.Now;
        private static DateTime tProgramstart = DateTime.Now;

        private static void Main(string[] args)
        {
            CreateShapeFile(args);
            //test();
        }

       

        public static void test()
        {

            OracleConnection con = new OracleConnection("user id=mams_dev;password=mams;data source=xe");
            List<GeoInfo> geoInfoList = new List<GeoInfo>();
            OracleCommand cmd = new OracleCommand();
            cmd.Connection = con;
            con.Open();
            cmd.CommandType = CommandType.Text;
            cmd.Parameters.Clear();
            cmd.CommandText = " select sgs_id,shape from sdo_segments_all ";
            
            using (OracleDataReader readerGeoInfo = cmd.ExecuteReader())
            {
                while (readerGeoInfo.Read())
                {
                    GeoInfo geoInfo = new GeoInfo();
                    if (!readerGeoInfo.IsDBNull(0))
                    {
                        geoInfo.Id = readerGeoInfo.GetDecimal(0);
                    }
                    if (!readerGeoInfo.IsDBNull(1))
                    {
                        geoInfo.Geo = (SdoGeometry)readerGeoInfo.GetValue(1);
                        decimal[] coords = geoInfo.Geo.OrdinatesArray;
                        for (int i = 0; i < coords.Length-1; i+=2)
			            {
			                if (i <= coords.Length-1)
                                Console.WriteLine("X: " + coords[i].ToString() + " Y: " + coords[i+1]);
			            }
                    }
                    geoInfoList.Add(geoInfo);
                }
                readerGeoInfo.Close();
            }
            con.Close();
        }

        private static List<GeoInfo> getGeomInfo(OracleConnection conn, string select, string spatial_column)
        {
            List<GeoInfo> geoInfoList = new List<GeoInfo>();
            if (conn.State == ConnectionState.Closed)
            { conn.Open(); }
            OracleCommand cmd = new OracleCommand();
            cmd.Connection = conn;
            cmd.CommandType = CommandType.Text;
            cmd.Parameters.Clear();
            cmd.CommandText = select;

            using (OracleDataReader readerGeoInfo = cmd.ExecuteReader())
            {
                while (readerGeoInfo.Read())
                {
                    GeoInfo geoInfo = new GeoInfo();
                    if (!readerGeoInfo.IsDBNull(0))
                    {
                        geoInfo.Id = readerGeoInfo.GetDecimal(0);
                    }
                    if (!readerGeoInfo.IsDBNull(1))
                    {
                        geoInfo.Geo = (SdoGeometry)readerGeoInfo[spatial_column];
                    }
                    geoInfoList.Add(geoInfo);
                }
                readerGeoInfo.Close();
            }
            return geoInfoList;
        }

        public static void CreateShapeFile(string[] args)
        {
            string pkValue = string.Empty;
            bool pkValIsString = false;
            bool isSdoPoint = false;

            if (args == null || args.Length < 5)
            {
                Console.Write("\nUsage:  sdo2shp <username/password>@dbalias> <spatial_table_name> <PK_col> <shape_col> <shapefile> [\"optional_where_clause\"]\n\nPress any key to exit");
                Console.ReadLine();
                return;
            }
            System.Reflection.Assembly assembly = System.Reflection.Assembly.GetExecutingAssembly();
            string version = assembly.GetName().Version.ToString();
            Console.Write("\nORA2SHP Oracle SDO to Shapefile Utility. Version: " + version+"\n");
            Console.Write("====================================================================\n");


            string connectionstring = Utils.ParseConnectionString(args[0].ToString());
            string where_clause = string.Empty;
            try
            {
                where_clause = args[5].ToString().Trim('"');
            }
            catch (Exception)
            { }

            if (where_clause != string.Empty)
            {
                if (!where_clause.ToUpper().StartsWith("WHERE"))
                    where_clause = " where " + where_clause;
            }

            string outShpFile = args[4].ToString();
            string tab_name = args[1].ToString();
            string pk_column = args[2].ToString();
            string sdo_column = args[3].ToString();
            if (outShpFile.ToUpper().EndsWith(".SHP"))
                outShpFile = outShpFile.Substring(0, outShpFile.Length - 4);

            OracleConnection oracon = new OracleConnection(connectionstring);
            List<GeoInfo> lShpInfo = new List<GeoInfo>();
            try
            {
                oracon.Open();
                string selFirstShp = string.Empty;
                if (where_clause== string.Empty)
                    selFirstShp = "select " + args[2].ToString() + "," + args[3].ToString() + " from " + args[1] + " where rownum = 1";
                else
                    selFirstShp="select " + pk_column + ","+ sdo_column + " from " + tab_name + " " + where_clause + " and rownum = 1";
                try
                {


                    lShpInfo = getGeomInfo(oracon, selFirstShp, sdo_column);
                    if (lShpInfo.Count == 0)
                    {
                        Console.WriteLine("\nNo rows found it table {0}", args[1]);
                        Console.ReadLine();
                        oracon.Close(); oracon.Dispose();
                        return;
                    }
                }
                catch (Exception ex)
                {

                    Console.WriteLine("\nORA2SHP ERROR:\n" + ex.Message + "\n" + ex.StackTrace);
                }
                finally
                {
                    
                }
                //Check if its an SDO point geometry
                GeoInfo geomInfo = lShpInfo[0];
                isSdoPoint = isSDOPointType(geomInfo);

                

                //Initialize shape type
                ShapeLib.ShapeType shpType = ShapeLib.ShapeType.Point;
                try
                {
                    shpType = GetShapeType(geomInfo);
                }
                catch (Exception ex)
                {
                    Console.Write(ex.Message);
                    Console.ReadLine();
                    return;
                }

                IntPtr hShp = ShapeLib.SHPCreate(outShpFile, shpType);
                if (hShp.Equals(IntPtr.Zero))
                {
                    Console.WriteLine("\nCould not create {0}.shp\nProbable cause: You do not have permissions or file is in use by another process\n\nPress any key to exit", outShpFile);
                    Console.ReadLine();
                    return;
                }

                string sqlselect = string.Empty;
                if (where_clause == string.Empty)
                {
                    sqlselect = @"SELECT " + GetColumnNames(oracon, tab_name) + ", " + sdo_column +" from " + tab_name;
                }
                else
                {
                    sqlselect = @"SELECT " + GetColumnNames(oracon, tab_name) + ", " + sdo_column + " from " + tab_name + " " + where_clause;
                }

                OracleCommand command1 = oracon.CreateCommand();
                command1.CommandText = sqlselect;
                OracleDataReader dr = command1.ExecuteReader();

                int iShape = 0;
                IntPtr hDbf = IntPtr.Zero;
                DataTable dt = new DataTable();
                System.Collections.Hashtable ht = new System.Collections.Hashtable();
                //#if (DEBUG==true)

                //                Console.WriteLine("Before Loop Elapsed Time: " + (DateTime.Now - tstart).ToString());
                tstart = DateTime.Now;
                //#endif

                #region create dbf

                // create dbase file
                hDbf = ShapeLib.DBFCreate(outShpFile);

                if (!hDbf.Equals(IntPtr.Zero))
                {
                    dt = dr.GetSchemaTable(); //Get table metadata
                    dt.Columns.Add("dBaseName");
                }
                int ordinal1 = 0;
                foreach (DataRow row in dt.Rows)
                {
                    string name = row["ColumnName"].ToString().ToUpper();

                    if (name.Length > 10)  //Truncate column name to 10 chars
                        name = name.Substring(0, 10);
                    int i = 0;
                    while (ht.ContainsKey(name))  //Check if column name exists after truncation
                    {
                        string iVal = (i++).ToString();
                        if (name.Length + iVal.Length > 10)
                            name = name.Substring(0, 10 - iVal.Length) + iVal;
                        else
                            name = name + iVal;
                    }
                    ht.Add(name, ordinal1++);
                    row["dBaseName"] = name;
                    string type = row["DataType"].ToString();
                    switch (type)
                    {
                        case "System.Int32":
                        case "System.Int16":
                            ShapeLib.DBFAddField(hDbf, name, ShapeLib.DBFFieldType.FTInteger, 16, 0);
                            break;

                        case "System.String":
                            int len = Math.Min(255, int.Parse(row["ColumnSize"].ToString()));
                            ShapeLib.DBFAddField(hDbf, name, ShapeLib.DBFFieldType.FTString, len, 0);
                            if (name == args[1].ToString())
                            {
                                pkValIsString = true;
                            }
                            break;

                        case "System.Boolean":
                            ShapeLib.DBFAddField(hDbf, name, ShapeLib.DBFFieldType.FTLogical, 5, 0);
                            break;

                        case "System.Double":
                        case "System.Float":
                        case "System.Decimal":
                            int prec = int.Parse(row["NumericPrecision"].ToString());
                            int scale = int.Parse(row["NumericScale"].ToString());
                            ShapeLib.DBFAddField(hDbf, name, ShapeLib.DBFFieldType.FTDouble, prec, scale);
                            break;

                        case "System.DateTime":
                            ShapeLib.DBFAddField(hDbf, name, ShapeLib.DBFFieldType.FTDate, 8, 0);
                            break;

                        default:
                            ht.Remove(name);
                            row["dBaseName"] = null;
                            ordinal1--;
                            break;
                    }
                }

                #endregion create dbf

                ShapeCreator sc = new ShapeCreator();

                IntPtr pShp = new IntPtr();

                
                while (dr.Read())
                {
                    //Console.Write("Processing record ..." + iShape.ToString() +".");
                    pkValue = dr[args[2]].ToString();

                    OraShape shp = new OraShape();
                    ShapePartInfo shpInfo = new ShapePartInfo();

                    try
                    {
                        GeoInfo geoInfo = new GeoInfo();
                        geoInfo.Geo = (SdoGeometry)dr[sdo_column];
                        shp = sc.getOraShape(geoInfo, shpType, Convert.ToInt32(dr[pk_column]));
                    }
                    catch (Exception)
                    {
                        continue;
                    }

                    if (!isSdoPoint)
                    {
                        try
                        {
                            shpInfo = sc.getShapePartInfo(geomInfo);
                        }
                        catch (Exception)
                        {
                            continue;
                        }

                        shp.nParts = shpInfo.nParts;
                        shp.PartType = null;
                        shp.PartStarts = shpInfo.PartStarts;

                        if (shp.MList == null || shp.MList.Length == 0)
                        {
                            if (shp.ZList == null || shp.ZList.Length == 0)
                            {
                                pShp = ShapeLib.SHPCreateObject(shpType, -1, shp.nParts, shp.PartStarts, null, shp.nVertices, shp.XList, shp.YList, null, null);
                            }
                            else
                            {
                                pShp = ShapeLib.SHPCreateObject(shpType, -1, shp.nParts, shp.PartStarts, null, shp.nVertices, shp.XList, shp.YList, shp.ZList, null);
                            }
                        }
                        else
                        {
                            pShp = ShapeLib.SHPCreateObject(shpType, -1, shp.nParts, shp.PartStarts, null, shp.nVertices, shp.XList, shp.YList, null, shp.MList);
                        }
                    }
                    else
                    {
                        shp = sc.CreatePointShape(geomInfo, shpType, pkValue);
                        if (shpType == ShapeLib.ShapeType.PointM)
                            pShp = ShapeLib.SHPCreateObject(shpType, -1, 0, null, null, 1, shp.XList, shp.YList, null, shp.MList);
                        else if (shpType == ShapeLib.ShapeType.PointZ)
                            pShp = ShapeLib.SHPCreateObject(shpType, -1, 0, null, null, 1, shp.XList, shp.YList, shp.ZList, null);
                        else
                            pShp = ShapeLib.SHPCreateObject(shpType, -1, 0, null, null, 1, shp.XList, shp.YList, null, null);
                    }
                    try //In case of invalid shapes
                    {
                        ShapeLib.SHPWriteObject(hShp, -1, pShp);
                    }
                    catch (Exception)
                    {
                        Console.WriteLine("Shape with " + args[2].ToString().ToUpper() + "= " + pkValue + " is invalid. Number of vertices: " + shp.nVertices.ToString() + " Number of parts: " + shp.nParts.ToString());
                        continue;
                    }
                    ShapeLib.SHPDestroyObject(pShp);

                    foreach (DataRow row in dt.Rows)
                    {
                        if (row["dBaseName"] == null || row["dBaseName"].ToString() == string.Empty)
                            continue;

                        int ordinal = (int)ht[row["dBaseName"].ToString()];
                        string fieldName = row["ColumnName"].ToString();
                        if (dr[fieldName] is DBNull)
                            continue;

                        switch (row["DataType"].ToString())
                        {
                            case "System.Int32":
                            case "System.Int16":
                                ShapeLib.DBFWriteIntegerAttribute(hDbf, iShape, ordinal, int.Parse(dr[fieldName].ToString()));
                                break;

                            case "System.String":
                                ShapeLib.DBFWriteStringAttribute(hDbf, iShape, ordinal, dr[fieldName].ToString());
                                break;

                            case "System.Boolean":
                                ShapeLib.DBFWriteLogicalAttribute(hDbf, iShape, ordinal, bool.Parse(dr[fieldName].ToString()));
                                break;

                            case "System.Double":
                            case "System.Float":
                            case "System.Decimal":
                                ShapeLib.DBFWriteDoubleAttribute(hDbf, iShape, ordinal, double.Parse(dr[fieldName].ToString()));
                                break;

                            case "System.DateTime":
                                DateTime date = DateTime.Parse(dr[fieldName].ToString());
                                ShapeLib.DBFWriteDateAttribute(hDbf, iShape, ordinal, date);
                                break;
                        }
                    }
                    iShape++;
                    //dr.NextResult();
                }
                Console.WriteLine("Converted " + iShape.ToString() + " shapes in: " + (DateTime.Now - tstart).ToString());

                // free resources
                ShapeLib.SHPClose(hShp);
                ShapeLib.DBFClose(hDbf);

                //Create projection file if needed
                CreatePrjFile(oracon, args[1].ToString(), args[3].ToString(), outShpFile);
                //Console.Write("Done.\n");
                Console.Write("\nCreated shapefile " + args[4].ToString() + " with " + iShape.ToString() + " records.\nTotal Elapsed Time: " + (DateTime.Now - tProgramstart).ToString() + "\nPress any key to exit.");
                Console.ReadLine();
            }
            catch (Exception ex)
            {
                Console.Write("ERROR: " + ex.Message + "\nPress any key to exit");
                Utils.WriteErrLog(ex);
                Console.ReadLine();
            }
            finally
            {
                //Close and free connection
                oracon.Close(); oracon.Dispose();
            }
        }

        

        protected static bool isSDOPointType(GeoInfo geomInfo)
        {
            if (geomInfo.Geo.Point== null)
                return false;
            else
                return true;
        }

        protected static ShapeLib.ShapeType GetShapeType(GeoInfo geomInfo)
        {
            //Console.Write("Determining shapetype....\n");
           
            string strGType = geomInfo.Geo.Sdo_Gtype.ToString();

            switch (strGType)
            {
                case "2001": //Point
                    return ShapeLib.ShapeType.Point;
                case "2005": //Point
                    return ShapeLib.ShapeType.MultiPoint;
                case "2002": //Line
                case "2006": //Multi-part Line
                    return ShapeLib.ShapeType.PolyLine;
                case "2003": //Polygon
                    return ShapeLib.ShapeType.Polygon;
                case "3001": //3D Point
                    return ShapeLib.ShapeType.PointZ;
                case "3006": //Mulit-part 3D Line
                    return ShapeLib.ShapeType.PolyLineZ;
                case "3003": //3D Polygon
                    return ShapeLib.ShapeType.PolygonZ;
                case "3301": //MPoint
                    return ShapeLib.ShapeType.PointM;
                case "3302": //MPolyline
                case "3002":
                    return ShapeLib.ShapeType.PolyLineM;
                default:
                    throw new Exception(strGType + " type shapes are not supported");
            }
        }

        protected static string GetColumnNames(OracleConnection oracon, string table_name)
        {
            //Console.Write("Determining column names....\n");
            string strColNames = string.Empty;

            if (oracon.State == ConnectionState.Closed)
            { oracon.Open(); }

            string sqlstring = @"select column_name from user_tab_columns where table_name='" + table_name.ToUpper() + "' and data_type <> 'SDO_GEOMETRY'";

            OracleCommand command = oracon.CreateCommand();
            command.CommandText = sqlstring;
            OracleDataReader rd = command.ExecuteReader();
            while (rd.Read())
            {
                strColNames = strColNames + "," + rd["column_name"].ToString();
            }
            strColNames = strColNames.Trim().TrimStart(',');
            //Console.Write("Done.\n");
            command.Dispose();
            return strColNames;
        }

        protected static void CreatePrjFile(OracleConnection oracon, string table_name, string shape_col, string shapefile)
        {
            if (oracon.State == ConnectionState.Closed)
            { oracon.Open(); }
            string sqlstring1 = @"select distinct to_char(s." + shape_col + ".SDO_SRID) as SRID, m.WKTEXT as PROJ_TEXT from " + table_name + " s, MDSYS.SDO_CS_SRS m where s." + shape_col + ".SDO_SRID =m.SRID";

            OracleCommand command1 = oracon.CreateCommand();
            command1.CommandText = sqlstring1;
            OracleDataReader rd = command1.ExecuteReader();
            int irow = 0;
            string sAllSrids = string.Empty;
            string strSRID = string.Empty;
            string strProjText = string.Empty;
            while (rd.Read())
            {
                strSRID = rd["SRID"].ToString();
                strProjText = rd["PROJ_TEXT"].ToString();
                sAllSrids = sAllSrids + "," + rd["SRID"].ToString();
                irow++;
            }

            if (irow == 0)
                return;

            if (irow > 1)
            {
                Console.Write("WARNING: Multiple SRIDs found in geometries (" + sAllSrids.Trim(',') + "\n");
                Console.Write("         SRID: " + strSRID + " will be used for the shapefile.\n");
            }

            FileInfo f = new FileInfo(shapefile + ".prj");
            StreamWriter w = f.CreateText();
            w.WriteLine(strProjText);
            w.Close();
        }
    }
}