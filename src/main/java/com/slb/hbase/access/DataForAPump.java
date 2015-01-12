package com.slb.hbase.access;

import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NavigableMap;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
//import org.apache.hadoop.hbase.filter.QualifierFilter;
//import org.apache.hadoop.hbase.filter.RowFilter;
//import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
//import org.apache.hadoop.hbase.KeyValue;
import org.apache.log4j.Logger;

import com.slb.hbase.connection.SingletonConnection;
import com.slb.pojo.TAG;
import com.slb.pojo.TAGComparator;


public class DataForAPump {

	private final static Logger logger = Logger.getLogger(DataForAPump.class);

	private final static int hourrounder = 3600000;

	private final static String T_COLUMN_FAMILY = "t";

	private final static String M_COLUMN_FAMILY = "m";

	private final static String PUMP_ID_CQ = "pumpIdNo";

	private final static String TAG_ID_CQ = "tagIdNo";

	private final static String TAG_METADATA_TABLE = "t_tag_meta_data";

	private final static String TAG_EVENTS_TABLE = "t_tag_events";

	private final static String RESULT_TAGS_OUTPUT_FILE = "results";

	private final static String RAW_TAGS_OUTPUT_FILE = "raw_tags";

	private final static String FILTERED_TAGS_OUTPUT_FILE = "filtered_tags";

	private static HashMap<String, String> pumpIdstoNames = null;

	private static HashMap<String, String> tagIdstoNames = null;

	private static final byte[] mask = new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0};

	public  boolean dumpResults = false;


	public static void main(String[] args) {

		try {

			if (args.length < 3) {

				System.out.println(" Usage <PumpId> <StartTime> <EndTime> <DumpResults> <DPFilter> <Epsilon> <Tag Id> ... <Tag Id>");

				System.exit(0);
			}

			String pumpName  = new String(args[0]);

			String startTime = new String(args[1]);

			String endTime = new String(args[2]);
			String startTimeStr = TimeStamp.getTimeStamp();
			logger.info("[" + startTimeStr + "] " +
					"main:: PumpName + StartTime + EndTime :: " + pumpName + " " + startTime + " " + endTime);

			boolean dumpResults = false;

			if (args.length > 3) {

				if (new Integer(args[3]).intValue() ==  1)
					dumpResults = true;
				else
					dumpResults = false;
			}

			boolean applyFilter = false;

			if (args.length > 4) {

				if (new Integer(args[4]).intValue() ==  1)
					applyFilter = true;
				else
					applyFilter = false;
			}

			Double epsilon = 0.0;

			if (args.length > 5)

				epsilon = Double.parseDouble(args[5]);

			logger.info("[" + TimeStamp.getTimeStamp() + "] " +
					"main:: Dump Results + ApplyFilter + epsilon :: " + dumpResults + applyFilter + " " + epsilon);

			List<String> tagList = new ArrayList<String>();

			for (int i=6; i < args.length; i++ )
			{
				tagList.add(new String(args[i]));

				logger.info("[" + TimeStamp.getTimeStamp() + "] " +
						"main:: Tag Name :: " + args[i]);
			}

			DataForAPump dfp = new DataForAPump();

			//String pumpName = "pump1";

			//tagList.add("Flow meter");

			//tagList.add("Discharge_Press");

			//String tagName = "Flow meter";

			//String startTime = "2014-10-16 01:00:00.000";

			//String endtime = "2014-10-18 01:00:00.000";

			//dfp.getAllDataForAPump(pumpName, startTime, endTtime);

			//dfp.getDataForSpecificTagForAPump(pumpName, tagName, startTime, endTtime);

			//dfp.getDataForSpecificTagsForAPump(pumpName, tagList, startTime, endTime);

			dfp.dumpResults = dumpResults;

			TreeSet<TAG> tagTreeSet = dfp.getDataForAPump(pumpName, startTime, endTime, tagList, applyFilter, epsilon);

			System.out.println("#########################################");
			System.out.println("Start Time:" + startTimeStr);
			System.out.println("End Time:" + TimeStamp.getTimeStamp());
			if(tagTreeSet != null)
				System.out.println("Result size :" + tagTreeSet.size());
			System.out.println("#########################################");


		}
		catch (Exception e) {

			e.printStackTrace(System.err);

			logger.error("[" + TimeStamp.getTimeStamp() + "] " + "main()..... ::" +  e.getMessage(), e);
		}
	}

//Obsolete, instead use getDataForAPump

	public TreeSet<TAG> getAllDataForAPump(String pumpname, String starttime, String endtime) throws Exception
	{
		try {

			logger.debug("[" + TimeStamp.getTimeStamp() + "] " + "DataForAPump::getAllDataForAPump....<pumpname><stime><endtime>" +
					pumpname + " " + starttime + " " + endtime);

			TreeSet<TAG> tSet = null;

			Scan scan = setupBaseScan(pumpname, starttime, endtime);

			HTableInterface tagtable = SingletonConnection.getInstance(TAG_EVENTS_TABLE);

			ResultScanner scanner = tagtable.getScanner(scan);

			tSet = emitResults(scanner);

			tagtable.close();

			return tSet;
		}
		catch (Exception e) {

			e.printStackTrace(System.err);

			logger.error("[" + TimeStamp.getTimeStamp() + "] " + "getAllDataForAPump..... ::" +  e.getMessage(), e);

			throw e;
		}
	}

	public TreeSet<TAG> getDataForAPump(String pumpname,  String starttime, String endtime,
										List<String> tags, boolean applyDPFltr, double epsilon) throws Exception
	{
		try {

			logger.debug("[" + TimeStamp.getTimeStamp() + "] " + "DataForAPump::getAllDataForAPump....<pumpname><stime><endtime>" +
					pumpname + " " + starttime + " " + endtime);

			TreeSet <TAG> tagSet = null;

			tagSet =  getDataForSpecificTagsForAPump(pumpname, tags, starttime, endtime);

			if (applyDPFltr && (tagSet.size() > 2)) {

				dumpTags(tagSet, RAW_TAGS_OUTPUT_FILE, pumpname);

				logger.info("[" + TimeStamp.getTimeStamp() + "] " + "getDataForAPump:: No of rawTags ::" + tagSet.size());

				TAG[] rawTags = tagSet.toArray(new TAG[tagSet.size()]);

				DouglasPeuckerFilter dpFilter = new DouglasPeuckerFilter(epsilon);

				TAG [] filteredTags = dpFilter.DPFilterTags(rawTags, 0, rawTags.length - 1);

				tagSet.clear();

				tagSet.addAll(Arrays.asList(filteredTags));

				dumpTags(tagSet, FILTERED_TAGS_OUTPUT_FILE, pumpname);

				logger.info("[" + TimeStamp.getTimeStamp() + "] " + "getDataForAPump:: No of Filtered Tags ::" + tagSet.size());

			}

			return tagSet;
		}
		catch (Exception e) {

			e.printStackTrace(System.err);

			logger.error("[" + TimeStamp.getTimeStamp() + "] " + "getAllDataForAPump..... ::" +  e.getMessage(), e);

			throw e;
		}
	}

// Left temporarily for backward compatibility, will be deprecated in the future. Use getDataForAPump() instead

	public TreeSet<TAG> getDataForSpecificTagsForAPump(String pumpname, List<String> tags, String starttime, String endtime) throws Exception
	{
		try {

			TreeSet <TAG> tagSet = null;

			Scan scan = setupBaseScan(pumpname, starttime, endtime);

			if ((tags != null) && (tags.size() > 0))
			{
				FilterList fbaselist = new FilterList(FilterList.Operator.MUST_PASS_ONE);

				List<Integer> tagids = new ArrayList<Integer>();

				for (String tag : tags) {

					int tagid = new Integer(tagIdstoNames.get(tag));

					tagids.add(tagid);
				}

				fbaselist.addFilter(setupFuzzyFilter(tagids, new byte[16]));

//				QualifierFilter cqFilter = new QualifierFilter()

				scan.setFilter(fbaselist);
			}

			HTableInterface tagtable = SingletonConnection.getInstance(TAG_EVENTS_TABLE);

			ResultScanner scanner = tagtable.getScanner(scan);

			tagSet = emitResults(scanner);

			tagtable.close();

			return tagSet;
		}
		catch (Exception e) {

			e.printStackTrace(System.err);

			logger.error("[" + TimeStamp.getTimeStamp() + "] " + "getDataForSpecificTagsForAPump..... ::" +  e.getMessage(), e);

			throw e;
		}
	}

	private Scan setupBaseScan(String pumpname, String starttime, String endtime) throws Exception
	{
		try {

			logger.debug("[" + TimeStamp.getTimeStamp() + "] " + "DataForAPump::setupBaseScan <pumpname><stime><etime> "
					+ pumpname + " " + starttime + " " + endtime );

			int pumpId = new Integer(pumpIdstoNames.get(pumpname));

			logger.debug("[" + TimeStamp.getTimeStamp() + "] " + "DataForAPump::setupBaseScan PumpId Retrieved "
					+ pumpname+ " " + pumpId);

			long[] stimes = formattime(starttime);

			long[] etimes = formattime(endtime);

			logger.debug("[" + TimeStamp.getTimeStamp() + "] " + "DataForAPump::setupBaseScan <stime> <etime> "
					+ stimes[0] + " " + etimes[0]);

			byte[] skey = new byte[16];

			byte[] ekey = new byte[16];

			Bytes.putInt(skey, 0, pumpId);

			Bytes.putLong(skey, 4, stimes[0]);

			Bytes.putInt(skey, 12, 0);

			Bytes.putInt(ekey, 0, pumpId);

			Bytes.putLong(ekey, 4, etimes[0] + 1);

			Bytes.putInt(ekey, 12, Integer.MAX_VALUE);

			Scan scan = new Scan(skey, ekey);

			scan.addFamily(Bytes.toBytes(T_COLUMN_FAMILY));

			return scan;
		}
		catch (Exception e)
		{
			e.printStackTrace(System.err);

			logger.error("[" + TimeStamp.getTimeStamp() + "] " + "setupBaseScan..... ::" +  e.getMessage(), e);

			throw e;
		}
	}

	private FuzzyRowFilter setupFuzzyFilter(List<Integer> tagids, byte[] datakey) throws Exception {

		try {

			List<Pair<byte[], byte[]>> pairs= new ArrayList<Pair<byte[], byte[]>>();

			for (int tagid : tagids) {

				logger.debug("[" + TimeStamp.getTimeStamp() + "] " +
						"setupFuzzyFilter Tag Id :: " + tagid);

				byte[] rowkey = Arrays.copyOf(datakey, datakey.length);

				Bytes.putInt(rowkey, 12, tagid);

				pairs.add(new Pair(rowkey, mask));
			}

			FuzzyRowFilter frowFilter1 = new FuzzyRowFilter(pairs);

			return frowFilter1;
		}
		catch (Exception e)
		{
			e.printStackTrace(System.err);

			logger.error("[" + TimeStamp.getTimeStamp() + "] " + "setupFuzzyFilter..... ::" +  e.getMessage(), e);

			throw e;
		}
	}

	private TreeSet<TAG> emitResults(ResultScanner scanner) throws Exception
	{
		try {

			FileWriter writer = null;

			if (dumpResults) {

				writer = new FileWriter(RESULT_TAGS_OUTPUT_FILE,false);

				String outputLine = "PumpId + TimeStamp + TagId + Column Family + Key + Value  +  VersionTimeStamp";

				writer.append(outputLine);

				writer.append('\n');

				writer.flush();
			}

			Iterator<Result> iterator = scanner.iterator();

			int noOfRows = 0;

			TreeSet<TAG> tagSet = new TreeSet<TAG>(new TAGComparator());

			while (iterator.hasNext())
			{
				noOfRows++;

				Result next = iterator.next();

				byte[] rowKey = next.getRow();

				int pumpId = Bytes.toInt(rowKey, 0);

				String pumpName = pumpIdstoNames.get(new Integer(pumpId).toString());

				long timeStampBase = Bytes.toLong(rowKey, 4);

				int tagId = Bytes.toInt(rowKey, 12);

				String tagName = tagIdstoNames.get(new Integer(tagId).toString());

				for(Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> columnFamilyMap : next.getMap().entrySet())
				{
					String columnFamily = Bytes.toString(columnFamilyMap.getKey());

					for (Entry<byte[], NavigableMap<Long, byte[]>> entryVersion : columnFamilyMap.getValue().entrySet())
					{
						for (Entry<Long, byte[]> entry : entryVersion.getValue().entrySet())
						{
							long timeStampOffset = Bytes.toLong(entryVersion.getKey());

							String timeStampVersion = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss:SSS").format(entry.getKey());

							long timeStamp = timeStampBase + timeStampOffset;

							String value; Double dValue = 0.0;

							if ("t".equalsIgnoreCase(columnFamily) || "q".equalsIgnoreCase(columnFamily))
							{
								dValue = Bytes.toDouble(entry.getValue());

								value = Double.toString(dValue);

							} else {

								value = Bytes.toString(entry.getValue());
							}

							//TAG tag = new TAG(tagName, timeStamp, tagId, dValue);

							tagSet.add(new TAG(tagName, timeStamp, tagId, dValue));

							if (dumpResults) {

								writer.append("<" + pumpName + "##" +
										new SimpleDateFormat("yyyy:MM:dd HH").format(timeStamp) + "##" + tagName + "##" +
										columnFamily + "##" +
										new SimpleDateFormat("mm:ss:SSS").format(timeStamp) + "##" + value
										+ "##" + timeStampVersion + ">");

								writer.append('\n');

								writer.append('\n');

								writer.flush();
							}
						}
					}
				}
			}

			logger.info("[" + TimeStamp.getTimeStamp() + "] " +
					"emitResults::No of tag_events Rows Scanned :: " + noOfRows);

			if (dumpResults)
				writer.close();

			return tagSet;
		}
		catch (Exception e)
		{
			logger.error("[" + TimeStamp.getTimeStamp() + "] " +
					"emitResults::" + e.getMessage(), e);

			e.printStackTrace();

			throw e;
		}
	}

	private long[] formattime(String timestamp) throws Exception
	{
		try {

			Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.ENGLISH).parse(timestamp);

			long timeinms = date.getTime();

			long hourtime = timeinms - (timeinms % hourrounder);

			long minutesecondmstime = timeinms % hourrounder;

			long[] output = new long[2];

			output[0] = hourtime;

			output[1] = minutesecondmstime;

			return output;
		}
		catch (Exception e) {

			e.printStackTrace(System.err);

			logger.error("[" + TimeStamp.getTimeStamp() + "] " + "formattime..... ::" +  e.getMessage(), e);

			throw e;
		}
	}

	private void getPumpsAndTagsInfo() throws Exception
	{
		try {

			Scan scan = new Scan();

			HTableInterface tagtable = SingletonConnection.getInstance(TAG_METADATA_TABLE);

			ResultScanner scanner = tagtable.getScanner(scan);

			DataForAPump.pumpIdstoNames = new HashMap<String, String>();

			DataForAPump.tagIdstoNames = new HashMap<String, String>();

			int noOfRows =0;

			for (Result result : scanner) {

				noOfRows++;

				String rowKey = Bytes.toString(result.getRow());

				if (rowKey.startsWith("P_")) {

					String pumpName = rowKey.substring(2);

					int pumpId = Bytes.toInt(result.getValue(Bytes.toBytes(M_COLUMN_FAMILY), Bytes.toBytes(PUMP_ID_CQ)));

					DataForAPump.pumpIdstoNames.put(pumpName, new Integer(pumpId).toString());

					DataForAPump.pumpIdstoNames.put(new Integer(pumpId).toString(), pumpName);
				}

				if (rowKey.startsWith("T_")) {

					String tagName = rowKey.substring(2);

					int pumpId = Bytes.toInt(result.getValue(Bytes.toBytes(M_COLUMN_FAMILY), Bytes.toBytes(TAG_ID_CQ)));

					DataForAPump.tagIdstoNames.put(tagName, new Integer(pumpId).toString());

					DataForAPump.tagIdstoNames.put(new Integer(pumpId).toString(), tagName);
				}

			}

			logger.debug("[" + TimeStamp.getTimeStamp() + "] " +
					"getPumpsAndTagsInfo::No Of tag_meta_data Rows Scanned :: " + noOfRows);

			tagtable.close();
		}
		catch (Exception e)
		{
			logger.error("[" + TimeStamp.getTimeStamp() + "] " +
					"getPumpsAndTagsInfo::" + e.getMessage(), e);
			throw e;
		}
	}

	private void dumpTags(TreeSet<TAG> tags, String fileName, String pumpName)
	{
		try {

			if (!dumpResults)
				return;

			FileWriter writer = new FileWriter(fileName, false);

			Iterator<TAG> iterator = tags.iterator();

			while (iterator.hasNext())
			{
				TAG tag = iterator.next();

				writer.append("<" + pumpName + "##" +
						new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(tag.ts) +
						"##" + tagIdstoNames.get(new Integer(tag.Id).toString()) + "##" +
						tag.value);

				writer.append('\n');

				writer.append('\n');

				writer.flush();
			}

			writer.flush();

			writer.close();
		}
		catch (Exception e) {

			e.printStackTrace(System.err);

			logger.error("[" + TimeStamp.getTimeStamp() + "] " + "dumpTags::" +  e.getMessage(), e);
		}
	}

	public DataForAPump()
	{
		try {

			if ( (DataForAPump.pumpIdstoNames == null) || (DataForAPump.tagIdstoNames == null))

				getPumpsAndTagsInfo();
		}
		catch (Exception e) {

			e.printStackTrace(System.err);

			logger.error("[" + TimeStamp.getTimeStamp() + "] " + "DataForAPump::" +  e.getMessage(), e);
		}
	}

}

