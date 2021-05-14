package io.mycat.statistic.histogram;

import org.junit.Assert;
import org.junit.Test;

public class MySQLHistogramParserTest {

    @Test
    public void testEquiHeight(){
        MySQLHistogram  histogram = MySQLHistogramParser.parse("{\n" +
                "  // Last time the histogram was updated. As of now, this means \"when the\n" +
                "  // histogram was created\" (incremental updates are not supported). Date/time\n" +
                "  // is given in UTC.\n" +
                "  // -- J_DATETIME\n" +
                "  \"last-updated\": \"2015-11-04 15:19:51.000000\",\n" +
                "\n" +
                "  // Histogram type. Always \"equi-height\" for equi-height histograms.\n" +
                "  // -- J_STRING\n" +
                "  \"histogram-type\": \"equi-height\",\n" +
                "\n" +
                "  // Histogram buckets. This will always be at least one bucket.\n" +
                "  // -- J_ARRAY\n" +
                "  \"buckets\":\n" +
                "  [\n" +
                "    [\n" +
                "      // Lower inclusive value.\n" +
                "      // -- Data type depends on the source column.\n" +
                "      \"0\",\n" +
                "\n" +
                "      // Upper inclusive value.\n" +
                "      // -- Data type depends on the source column.\n" +
                "      \"002a38227ecc7f0d952e85ffe37832d3f58910da\",\n" +
                "\n" +
                "      // Cumulative frequence\n" +
                "      // -- J_DOUBLE\n" +
                "      0.001978728666831561,\n" +
                "\n" +
                "      // Number of distinct values in this bucket.\n" +
                "      // -- J_UINT\n" +
                "      10\n" +
                "    ]\n" +
                "  ]\n" +
                "}");
        Assert.assertEquals("MySQLHistogram(lastUpdated=2015-11-04T15:19:51, equiHeight=true, mySQLBuckets=[MySQLBucket(lowerInclusiveValue=0, upperInclusiveValue=002a38227ecc7f0d952e85ffe37832d3f58910da, cumulativeFrequence=0.001978728666831561, numberOfDistinctValues=0.001978728666831561)])",
                histogram.toString());

    }

    @Test
    public void testSingleton(){
        MySQLHistogram histogram = MySQLHistogramParser.parse("{\n" +
                "  // Last time the histogram was updated. As of now, this means \"when the\n" +
                "  // histogram was created\" (incremental updates are not supported). Date/time\n" +
                "  // is given in UTC.\n" +
                "  // -- J_DATETIME\n" +
                "  \"last-updated\": \"2015-11-04 15:19:51.000000\",\n" +
                "\n" +
                "  // Histogram type. Always \"singleton\" for singleton histograms.\n" +
                "  // -- J_STRING\n" +
                "  \"histogram-type\": \"singleton\",\n" +
                "\n" +
                "  // Histogram buckets. This will always be at least one bucket.\n" +
                "  // -- J_ARRAY\n" +
                "  \"buckets\":\n" +
                "  [\n" +
                "    [\n" +
                "      // Value value.\n" +
                "      // -- Data type depends on the source column.\n" +
                "      42,\n" +
                "\n" +
                "      // Cumulative frequence\n" +
                "      // -- J_DOUBLE\n" +
                "      0.001978728666831561,\n" +
                "    ]\n" +
                "  ]\n" +
                "}");
        Assert.assertEquals("MySQLHistogram(lastUpdated=2015-11-04T15:19:51, equiHeight=false, mySQLBuckets=[MySQLBucket(lowerInclusiveValue=42, upperInclusiveValue=null, cumulativeFrequence=0.001978728666831561, numberOfDistinctValues=null)])",
                histogram.toString());
    }
}