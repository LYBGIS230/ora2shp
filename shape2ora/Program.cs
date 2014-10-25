using System;
using System.Collections.Generic;
using System.Text;
using MapTools;
using System.Data;
using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;
using System.Runtime.InteropServices;
using orashpUtils;

namespace shp2ora
{
    class Program
    {
        static void Main(string[] args)
        {
            ConvertToOra(args);
        }

        public static void ConvertToOra(string[] args)
        {
            string pkValue = string.Empty;
            if (args == null || args.Length < 4)
            {
                Console.Write("\nUsage:  shape2ora <username/password>@dbalias> <spatial_table_name> <shape_col> <shapefile> <srid> \n\nPress any key to exit");
                Console.ReadLine();
                return;
            }
            string connectionstring = Utils.ParseConnectionString(args[0].ToString());
            string spatial_table = args[1].ToString();
            string shape_col = args[2].ToString();
            string inShapeFile = args[3].ToString();
            string strSRID = "NULL";
            if (args.Length == 5)
            {
                strSRID = args[4].ToString();
            }

            if (inShapeFile.ToUpper().EndsWith(".SHP"))
                inShapeFile = inShapeFile.Substring(0, inShapeFile.Length - 4);

            IntPtr hShp = ShapeLib.SHPOpen(inShapeFile, "rb");
            IntPtr hDbf = ShapeLib.DBFOpen(inShapeFile, "rb");

            if (hDbf.Equals(IntPtr.Zero))
            {
                Console.WriteLine("\nCould not open {0}.dbf\nProbable cause: You do not have permissions or filename is incorrect\n\nPress any key to exit", inShapeFile);
                Console.ReadLine();
                return;
            }

            OracleConnection oracon = new OracleConnection(connectionstring);
            oracon.Open();
            try
            {
                //Check if table exists
                bool tabExists = TabExists(oracon, spatial_table);
                
                //Get dbf info
                int recCount = ShapeLib.DBFGetRecordCount(hDbf);
                // get shape info
                double[] minXY = new double[4];
                double[] maxXY = new double[4];
                int nEntities = 0;
                ShapeLib.ShapeType shapeType = 0;
                ShapeLib.SHPGetInfo(hShp, ref nEntities, ref shapeType, minXY, maxXY);

                string sqlCreateTab = @"CREATE TABLE " + spatial_table.ToUpper() + "(";
               
                bool flag = true;
                for (int i = 0; i < nEntities; i++)
                {

                    if (flag)
                    {
                        sqlCreateTab = sqlCreateTab + GetShapeColumnNames(ref hDbf);
                        sqlCreateTab = sqlCreateTab + shape_col + "\tMDSYS.SDO_GEOMETRY\n)";
                        if (!tabExists)
                        {
                            //EXECUTE table creation sql
                            ExecuteStatement(oracon, sqlCreateTab);
                        }
                        flag = false;
                    }
                    #region Deal With Geometry
                    IntPtr pshpObj = ShapeLib.SHPReadObject(hShp, i);

                    // Get the SHPObject associated with our IntPtr pshpObj
                    // We create a new SHPObject in managed code, then use Marshal.PtrToStructure
                    // to copy the unmanaged memory pointed to by pshpObj into our managed copy.
                    ShapeLib.SHPObject shpObj = new ShapeLib.SHPObject();
                    Marshal.PtrToStructure(pshpObj, shpObj);

                    //Number of parts
                    int nParts = shpObj.nParts;

                    //Part starts List
                    int[] partStarts = new int[nParts];

                    //Coordinate arrays
                    double[] xCoord = new double[shpObj.nVertices];
                    double[] yCoord = new double[shpObj.nVertices];
                    double[] zCoord = new double[shpObj.nVertices];
                    double[] MCoord = new double[shpObj.nVertices];

                    // Use Marshal.Copy to copy the memory pointed 
                    // to by shpObj.padfX and shpObj.padfX (each an IntPtr) to an actual array.
                    Marshal.Copy(shpObj.padfX, xCoord, 0, shpObj.nVertices);
                    Marshal.Copy(shpObj.padfY, yCoord, 0, shpObj.nVertices);
                    if (shapeType == ShapeLib.ShapeType.MultiPointM || shapeType == ShapeLib.ShapeType.PointM || shapeType == ShapeLib.ShapeType.PolygonM || shapeType == ShapeLib.ShapeType.PolyLineM)
                    {
                        Marshal.Copy(shpObj.padfM, zCoord, 0, shpObj.nVertices);
                    }
                    if (shapeType == ShapeLib.ShapeType.MultiPointZ || shapeType == ShapeLib.ShapeType.PointZ || shapeType == ShapeLib.ShapeType.PolygonZ || shapeType == ShapeLib.ShapeType.PolyLineZ)
                    {
                        Marshal.Copy(shpObj.padfZ, zCoord, 0, shpObj.nVertices);
                    }
                    if (nParts > 0)
                    {
                        Marshal.Copy(shpObj.paPartStart, partStarts, 0, nParts);
                    }
                    string sqlInsertShape = " MDSYS.SDO_GEOMETRY(";
                    string gType = GetGTYPE(shapeType, nParts);
                    string elem_info = "NULL";
                    string sdo_point = "NULL";
                    if (shapeType == ShapeLib.ShapeType.Point || shapeType == ShapeLib.ShapeType.PointM || shapeType == ShapeLib.ShapeType.PointZ)
                    {
                        sdo_point = "MDSYS.SDO_POINT_TYPE(" + xCoord[0].ToString() + "," + yCoord[0].ToString();
                        if (shapeType == ShapeLib.ShapeType.PointZ)
                        {
                            sdo_point = sdo_point + "," + zCoord[0].ToString();
                        }
                        else if (shapeType == ShapeLib.ShapeType.PointM)
                        {
                            sdo_point = sdo_point + "," + MCoord[0].ToString();
                        }
                        else
                        {
                            sdo_point = sdo_point + ", NULL";
                        }
                        sdo_point = sdo_point + ")";
                    }
                    else
                    {
                        elem_info = GetElemInfoString(shapeType, nParts, partStarts, shpObj.nVertices);
                    }
                    string vert_String = GetVerticesString(shapeType, xCoord, yCoord, zCoord, MCoord);

                    //Construct the geometry statement
                    sqlInsertShape = sqlInsertShape + gType + "," + strSRID + "," + sdo_point + "," + elem_info + "," + vert_String + ")";

                    # endregion
                    #region Deal with Attributes
                    string[] attrs = InsAttrSQL(ref hDbf, i);

                    string insStatement = "INSERT INTO " + spatial_table.ToUpper() + "\n"
                                           + "(" + attrs[0] + "," + shape_col + ")\n" +
                                           " VALUES (" + attrs[1] + "," + sqlInsertShape + ")";
                    //Do the insert
                    ExecuteStatement(oracon, insStatement);

                    #endregion
                }

                //Create user_sdo_geom_metadata and spatial index for a new table
                if (!tabExists)
                {
                    string usgm = GetUSGMString(shapeType, spatial_table, shape_col, minXY, maxXY);
                    ExecuteStatement(oracon, usgm);
                    string spidx = GetSPIDXString(spatial_table, shape_col);
                    ExecuteStatement(oracon, spidx);
                }

                // free resources
                ShapeLib.SHPClose(hShp);
                ShapeLib.DBFClose(hDbf);
                Console.Write("Done.\nPress any key to exit");
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
                try
                {
                    ShapeLib.SHPClose(hShp);
                    ShapeLib.DBFClose(hDbf);
                }
                catch (Exception)
                {}
                
                oracon.Close();
                oracon.Dispose();
            }


        }

        protected static string[] InsAttrSQL(ref IntPtr hDbf, int iShape)
        {
            string insAttrCols = string.Empty;
            string insAttrVals = string.Empty;
            int fieldCount = ShapeLib.DBFGetFieldCount(hDbf);
            int fieldWidth = 0;
            int numDecimals = 0;
            ShapeLib.DBFFieldType[] fieldTypes = new ShapeLib.DBFFieldType[fieldCount];
            
            string[] fieldNames = new string[fieldCount];
            for (int iField = 0; iField < fieldCount; iField++)
            {
                StringBuilder sb = new StringBuilder();
                fieldTypes[iField] = ShapeLib.DBFGetFieldInfo(hDbf, iField, sb, ref fieldWidth, ref numDecimals);
                fieldNames[iField] = sb.ToString();
                switch ((ShapeLib.DBFFieldType)fieldTypes[iField])
                {
                    case (ShapeLib.DBFFieldType.FTDouble):
                        insAttrCols = insAttrCols + ", " + fieldNames[iField];
                        if (ShapeLib.DBFIsAttributeNULL(hDbf, iShape, iField) == 0)
                        {
                            double val = ShapeLib.DBFReadDoubleAttribute(hDbf, iShape, iField);
                            insAttrVals = insAttrVals + ", " + val.ToString();
                            //Console.WriteLine("{0}({1}): {2}", fieldNames[iField], iShape, val);
                        }
                        else
                            //Console.WriteLine("{0}({1}) Is Null", fieldNames[iField], iShape);
                            insAttrVals = insAttrVals + ", NULL";
                        break;

                    case (ShapeLib.DBFFieldType.FTLogical):
                        insAttrCols = insAttrCols + ", " + fieldNames[iField];
                        if (ShapeLib.DBFIsAttributeNULL(hDbf, iShape, iField) == 0)
                        {
                            bool val = ShapeLib.DBFReadLogicalAttribute(hDbf, iShape, iField);
                            insAttrVals = insAttrVals + ", " + val.ToString();
                            //Console.WriteLine("{0}({1}): {2}", fieldNames[iField], iShape, val.ToString());
                        }
                        else
                            //Console.WriteLine("{0}({1}) Is Null", fieldNames[iField], iShape);
                            insAttrVals = insAttrVals + ", NULL";
                        break;

                    case (ShapeLib.DBFFieldType.FTInteger):
                        insAttrCols = insAttrCols + "," + fieldNames[iField];
                        if (ShapeLib.DBFIsAttributeNULL(hDbf, iShape, iField) == 0)
                        {
                            int val = ShapeLib.DBFReadIntegerAttribute(hDbf, iShape, iField);
                            insAttrVals = insAttrVals + ", " + val.ToString();
                            //Console.WriteLine("{0}({1}): {2}", fieldNames[iField], iShape, val);
                        }
                        else
                            //Console.WriteLine("{0}({1}) Is Null", fieldNames[iField], iShape);
                             insAttrVals = insAttrVals + ", NULL";
                        break;

                    case (ShapeLib.DBFFieldType.FTDate):
                        insAttrCols = insAttrCols + ", " + fieldNames[iField];
                        if (ShapeLib.DBFIsAttributeNULL(hDbf, iShape, iField) == 0)
                        {
                            DateTime val = ShapeLib.DBFReadDateAttribute(hDbf, iShape, iField);
                            if (val.Year == 1) // Deal with NULL dates (1/1/0001)
                                insAttrVals = insAttrVals + ", NULL"; 
                            else
                            insAttrVals = insAttrVals + ", " + "to_date('"+val.Month + "/" + val.Day + "/" + val.Year+"', 'MM/DD/YYYY')";
                            //Console.WriteLine("{0}({1}): {2}", fieldNames[iField], iShape, val.Month+"/"+val.Day+"/"+val.Year );
                        }
                        else
                            //Console.WriteLine("{0}({1}) Is Null", fieldNames[iField], iShape);
                            insAttrVals = insAttrVals + ", NULL";
                        break;

                    case (ShapeLib.DBFFieldType.FTInvalid):
                        Console.WriteLine("Field type is invalid");
                        break;

                    default:
                        insAttrCols = insAttrCols + ", " + fieldNames[iField];
                        if (ShapeLib.DBFIsAttributeNULL(hDbf, iShape, iField) == 0)
                        {
                            string val = ShapeLib.DBFReadStringAttribute(hDbf, iShape, iField);
                            val = val.Replace('\'', ' ');
                            insAttrVals = insAttrVals + ", '" + val.ToString()+"'";
                            //Console.WriteLine("{0}({1}): {2}", fieldNames[iField], iShape, val);
                        }
                        else
                            //Console.WriteLine("{0}({1}) Is Null", fieldNames[iField], iShape);
                            insAttrVals = insAttrVals + ", NULL";
                        break;
                }
            }
            insAttrVals = insAttrVals.Trim().Trim(',');
            insAttrCols = insAttrCols.Trim().Trim(',');
            string[] retString=new string[2];
            retString[0]=insAttrCols;
            retString[1]=insAttrVals;
            return retString;
        }

        protected static string GetShapeColumnNames( ref IntPtr hDbf)
        {
            string colString = string.Empty;
            int fieldCount = ShapeLib.DBFGetFieldCount(hDbf);
            ShapeLib.DBFFieldType[] fieldTypes = new ShapeLib.DBFFieldType[fieldCount];
            string[] fieldNames = new string[fieldCount];
            int fieldWidth = 0;
            int numDecimals = 0;
            for (int iField = 0; iField < fieldCount; iField++)
            {
                StringBuilder sb = new StringBuilder();
                fieldTypes[iField] = ShapeLib.DBFGetFieldInfo(hDbf, iField, sb, ref fieldWidth, ref numDecimals);
                fieldNames[iField] = sb.ToString();
                colString = colString + fieldNames[iField];
                switch (fieldTypes[iField])
                {
                    case ShapeLib.DBFFieldType.FTDate:
                        colString = colString + "\t DATE,\n";
                    break;
                    case ShapeLib.DBFFieldType.FTString:
                    colString = colString + "\t VARCHAR2(" + fieldWidth.ToString()+ "),\n";
                    break;
                    case ShapeLib.DBFFieldType.FTInteger:
                    colString = colString + "\t NUMBER(" + fieldWidth.ToString() + "),\n";
                    break;
                    case ShapeLib.DBFFieldType.FTDouble:
                    colString = colString + "\t NUMBER(" + fieldWidth.ToString() + "," + numDecimals.ToString() + "),\n";
                    break;
                }

            }
            return colString;
        }

     
        protected static string GetGTYPE(ShapeLib.ShapeType shpType, int nParts)
        {
            string strGtype =string.Empty;
            switch (shpType)
                {
                    case ShapeLib.ShapeType.MultiPoint:
                        return "2005";
                    case ShapeLib.ShapeType.Point:
                        return "2001";
                    case ShapeLib.ShapeType.PointM:
                        return "3301";
                    case ShapeLib.ShapeType.PointZ:
                        return "3001";
                    case ShapeLib.ShapeType.PolyLine:
                        if (nParts == 0 || nParts==1)
                            return "2002";
                        else return "2006";
                    case ShapeLib.ShapeType.PolyLineZ:
                        if (nParts == 0 || nParts == 1)
                            return "3002";
                        else return "3006";
                    case ShapeLib.ShapeType.PolyLineM:
                        if (nParts == 0 || nParts == 1)
                            return "3302";
                        else return "3306";
                    case ShapeLib.ShapeType.Polygon:
                        if (nParts == 0 || nParts == 1)
                            return "2003";
                        else return "2007";
                    case ShapeLib.ShapeType.PolygonZ:
                        if (nParts == 0 || nParts == 1)
                            return "3003";
                        else return "3007";
                    case ShapeLib.ShapeType.PolygonM:
                        if (nParts == 0 || nParts == 1)
                            return "3303";
                        else return "3307";
                default:
                        return null;
            }

    
        }

        protected static string GetElemInfoString(ShapeLib.ShapeType shpType, int noParts, int[] nPartStarts, int noVertices)
        {
            if (noParts == 0)
                return "NULL";
            string OraElemInfo = "SDO_ELEM_INFO_ARRAY(";
            for (int i = 0; i < noParts; i++)
            {
                OraElemInfo = OraElemInfo + (nPartStarts[i] + 1).ToString() + ",";
                switch (shpType)
                {
                    case ShapeLib.ShapeType.MultiPoint:
                    case ShapeLib.ShapeType.Point:
                    case ShapeLib.ShapeType.PointM:
                    case ShapeLib.ShapeType.PointZ:
                        OraElemInfo = "NULL";
                        break;
                    case ShapeLib.ShapeType.PolyLine:
                    case ShapeLib.ShapeType.PolyLineZ:
                    case ShapeLib.ShapeType.PolyLineM:
                        OraElemInfo = OraElemInfo +"2,1,";
                        break;
                    case ShapeLib.ShapeType.Polygon:
                    case ShapeLib.ShapeType.PolygonZ:
                    case ShapeLib.ShapeType.PolygonM:
                        OraElemInfo = OraElemInfo + "1003,1,";
                        break;
                }

            }

            OraElemInfo = OraElemInfo.TrimEnd(',');
            OraElemInfo = OraElemInfo + ")";
            return OraElemInfo;
        }

        protected static string GetVerticesString(ShapeLib.ShapeType shpType, double[] xCoord, double[] YCoord, double[] ZCoord, double[] MCoord)
        {
            string strVertices = string.Empty;
            bool is3D = false;
            switch (shpType)
            {
                case ShapeLib.ShapeType.Point:
                case ShapeLib.ShapeType.PointZ:
                case ShapeLib.ShapeType.PointM:
                    return "NULL";
                case ShapeLib.ShapeType.MultiPoint:
                case ShapeLib.ShapeType.PolyLine:
                case ShapeLib.ShapeType.Polygon:
                    is3D = false;
                    break;
                case ShapeLib.ShapeType.MultiPointZ:
                case ShapeLib.ShapeType.MultiPointM:
                case ShapeLib.ShapeType.PolyLineZ:
                case ShapeLib.ShapeType.PolyLineM:
                case ShapeLib.ShapeType.PolygonZ:
                case ShapeLib.ShapeType.PolygonM:
                    is3D = true;
                    break;
                default:
                   return "NULL";
            }

            if (!is3D)
            {
                for (int i = 0; i < xCoord.Length; i++)
                {
                    strVertices = strVertices + "," + xCoord[i] + "," + YCoord[i];
                }
            }
            else
            {
                if (shpType == ShapeLib.ShapeType.MultiPointZ || shpType == ShapeLib.ShapeType.PolyLineZ || shpType == ShapeLib.ShapeType.PolygonZ)
                {
                    for (int i = 0; i < xCoord.Length; i++)
                    {
                        strVertices = strVertices + "," + xCoord[i] + "," + YCoord[i] + "," + ZCoord[i];
                    }
                }
                else
                {
                    for (int i = 0; i < xCoord.Length; i++)
                    {
                        strVertices = strVertices + "," + xCoord[i] + "," + YCoord[i] + "," + MCoord[i];
                    }
                }
                
            }
            strVertices = strVertices.Trim(',');
            strVertices = "MDSYS.SDO_ORDINATE_ARRAY(" + strVertices + ")";
            return strVertices.Trim(',');
        }

        protected static bool TabExists(OracleConnection oracon, string table_name)
        {
            if (oracon.State == ConnectionState.Closed)
            { oracon.Open(); }
            string sqlstring = @"select count(*) from user_tables where table_name='" + table_name.ToUpper() + "'";

            OracleCommand command = oracon.CreateCommand();
            command.CommandText = sqlstring;
            try
            {
                int c = Convert.ToInt32(command.ExecuteScalar());
                if (c == 1) return true;
                else return false;
            }
            catch (Exception)
            {
                return false;
            }
            finally
            {
                command.Dispose();
            }
        }

        protected static string GetUSGMString(ShapeLib.ShapeType shpType, string table_name, string sp_col, double[] minXY, double[] maxXY)   
        {
            double minX = minXY[0];
            double minY = minXY[1];
            double maxX = maxXY[0];
            double maxY = maxXY[1];
        
            string usgm = string.Empty;
            if (shpType == ShapeLib.ShapeType.Point || shpType == ShapeLib.ShapeType.MultiPoint || shpType == ShapeLib.ShapeType.PolyLine || shpType == ShapeLib.ShapeType.Polygon)
            {
                usgm = @"INSERT INTO USER_SDO_GEOM_METADATA 
                    VALUES ('" + table_name.ToUpper() + @"', '" + sp_col.ToUpper() + @"',
                        MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X', " + minX.ToString()
                                                                     + @", " + maxX.ToString()
                                                                     + @", 0.005),
                                            MDSYS.SDO_DIM_ELEMENT('Y', " + minY.ToString() + ", " + maxY.ToString() + ", 0.005)),NULL)";
            }
            else
            {
                usgm = @"INSERT INTO USER_SDO_GEOM_METADATA 
                    VALUES ('" + table_name.ToUpper() + @"', '" + sp_col.ToUpper() + @"',
                        MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X', " + minX.ToString()
                                                                    + @", " + maxX.ToString()
                                                                    + @", 0.005),
                                            MDSYS.SDO_DIM_ELEMENT('Y', " + minY.ToString() + ", " + maxY.ToString() + @", 0.005)
                                            MDSYS.SDO_DIM_ELEMENT('Z', 0, 1000000, 0.005)),NULL)";

            }
           return usgm;
        }

        protected static string GetSPIDXString(string table_name, string sp_col)
        {
            string strSP = "CREATE INDEX " + table_name.ToUpper()+"_SPIDX ON " + table_name.ToUpper()+ "("+ sp_col+ @")
                            INDEXTYPE IS MDSYS.SPATIAL_INDEX";
            
            return strSP;
        }

        protected static void ExecuteStatement (OracleConnection oracon, string stmt)
        {
            if (oracon.State == ConnectionState.Closed)
            { oracon.Open(); }
            OracleCommand command = oracon.CreateCommand();
            command.CommandText = stmt;
            try
            {
                command.ExecuteNonQuery();
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                command.Dispose();
            }
        }
    }

}
