namespace ora2shp
{
    public class ShapePartInfo
    {
        public ShapePartInfo()
        {
        }

        /// <summary>
        /// Shape's PK
        /// </summary>
        public string shpPK
        {
            get;
            set;
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
    }
}