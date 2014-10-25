using System;
using System.Collections.Generic;
using System.Text;
using MapTools;

namespace ora2shp
{
    public class OraShape
    {
        public OraShape()
        {
            
        }
        /// <summary>
        /// Shape type
        /// </summary>
        public ShapeLib.ShapeType ShapeType
        {
            get;
            set;
        }

        /// <summary>
        /// Shape ID
        /// </summary>
        public int ShapeID
        {
            get;
            set;
        }

        /// <summary>
        /// Shape ID
        /// </summary>
        public string ShapePK
        {
            get;set;
        }

        /// <summary>
        /// Number of parts in shape
        /// </summary>
        public int nParts
        {
            get;
            set;
        }

        /// <summary>
        /// The list of zero based start vertices for the parts in this geometry.  The first should always be   zero.  This may be NULL if nParts is 0.
        /// </summary>
        public int[] PartStarts
        {
            get;
            set;
        }

        /// <summary>
        /// The type of each of the parts.  This is only meaningful for MULTIPATCH files.  For all other cases this maybe NULL, and will be assumed to be SHPP_RING.
        /// </summary>
        public MapTools.ShapeLib.PartType[] PartType
        {
            get;
            set;
        }

        /// <summary>
        /// The number of vertices being passed in XList,YList,ZList and MList
        /// </summary>
        public int nVertices
        {
            get;
            set;
        }

        /// <summary>
        /// An array of nVertices X coordinates of the vertices  for this geometry.
        /// </summary>
        public double[] XList
        {
            get;
            set;
        }

        /// <summary>
        /// An array of nVertices Y coordinates of the vertices  for this geometry.
        /// </summary>
        public double[] YList
        {
            get;
            set;
        }

        /// <summary>
        /// An array of nVertices Z coordinates of the vertices for this geometry.
        /// </summary>
        public double[] ZList
        {
            get;
            set;
        }

        /// <summary>
        /// An array of nVertices M coordinates of the vertices for this geometry.
        /// </summary>
        public double[] MList
        {
            get;
            set;
        }
    }
}
