package com.vbakh.s3.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class CollectionUtils {

    public static byte[] toByteArray(List<Integer> list) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        list.stream()
                .map(i -> i + "\n")
                .map(String::getBytes)
                .forEach(bytes -> {
                    try { baos.write(bytes); }
                    catch (IOException e) { throw new RuntimeException(e); }
                });
        return baos.toByteArray();
    }

    public static List<Integer> fromInputStream(InputStream input) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
            List<Integer> list = new ArrayList();
            String line = null;
            while ((line = reader.readLine()) != null) {
                list.add(Integer.valueOf(line));
            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
