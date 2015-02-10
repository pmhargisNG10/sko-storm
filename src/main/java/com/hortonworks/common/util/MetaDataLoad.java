package com.hortonworks.common.util;

import com.hortonworks.common.data.Pump;
import com.hortonworks.common.data.TagMetaData;
import com.hortonworks.hbase.persist.MetaData;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class MetaDataLoad {
	public static void main(String[] args) throws IOException, URISyntaxException {
		Path path = null;
		if(args.length == 1) {
//			path = Paths.get(new URI(args[0]));
			path = FileSystems.getDefault().getPath(args[0]);
		} else {
//		path = Paths.get(MetaDataLoad.class.getResource("/rt_meta_data.csv").getPath());
			path = Paths.get(MetaDataLoad.class.getResource("/meta_data.csv").getPath());
		}
		MetaData metaData = new MetaData();
		try {
			BufferedReader reader = Files.newBufferedReader(path,
					StandardCharsets.UTF_8);
			String line = null;
			while ((line = reader.readLine()) != null) {
				System.out.println(line);
				TagMetaData tmd = new TagMetaData();
				String[] fields = line.split(",");
                Pump pump = new Pump();
                pump.setPumpId(fields[1]);
				pump.setPumpName(fields[2]);
				pump.setPumpType(fields[3]);
				//Get pump no
                int pumpIdNo = metaData.getPumpId(pump);

				tmd.setPump(pump.getPumpId());
				tmd.setTag(fields[5]);
				tmd.setTagType(fields[6]);
				tmd.setTagName(fields[7]);

				tmd.setUom(fields[8]);
				tmd.setHighHighValue(Double.valueOf(fields[9]));
				tmd.setHighValue(Double.valueOf(fields[10]));
				tmd.setLowValue(Double.valueOf(fields[11]));
				tmd.setLowLowValue(Double.valueOf(fields[12]));
				tmd.setDesc("DESC Deprecated");
				tmd.setPumpId(pumpIdNo);

				int tagId = metaData.getTagId(tmd);
				metaData.createMapping(pumpIdNo, tmd.getPump(), tagId, tmd.getTag());
			}
		} catch (IOException ioe) {
			System.err.println(ioe);
		}
	}

}
