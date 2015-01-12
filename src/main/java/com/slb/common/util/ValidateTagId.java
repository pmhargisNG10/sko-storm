package com.slb.common.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.slb.common.data.TagEvent;
import com.slb.hbase.lookup.MetaLookup;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by dpramodv on 12/5/14.
 */
public class ValidateTagId {
    private static final Gson GSON = new
            GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'").create();
    private static void validateTagId(String tagId) throws IOException {
        MetaLookup metaLookup = new MetaLookup();
//        metaLookup.getMetaDataforTag(tagId);
        metaLookup.getTagMetaDataforTag(tagId);
    }

    public static void main(String... args) throws IOException {
        Path path = Paths.get(MetaDataLoad.class.getResource("/rt_sample_data.txt").getPath());

        BufferedReader reader = Files.newBufferedReader(path,
                StandardCharsets.UTF_8);
        String line = null;
        while ((line = reader.readLine()) != null) {
            TagEvent tagEvent = GSON.fromJson(line, TagEvent.class);
            System.out.println(tagEvent);
            try {
                validateTagId(tagEvent.getTagId());
            } catch (NullPointerException n) {
                System.out.println(n);
            }
        }
    }
}
