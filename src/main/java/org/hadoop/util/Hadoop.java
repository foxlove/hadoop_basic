package org.hadoop.util;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Hadoop {

	public JSONObject getHomeDir(FileSystem fs) throws IOException {
		JSONObject json = new JSONObject();
		Path homeDir = fs.getHomeDirectory();
		homeDir = FSUtils.convertPathToHoop(homeDir, ServerInfo.HDFS_BASE_URL);
		json.put("homeDir", homeDir.toString());
		return json;
	}

	public JSONObject makeDir(FileSystem fs, Path path) throws IOException {
		JSONObject json = new JSONObject();
		boolean result = fs.mkdirs(path);
		json.put("result", result);
		return json;
	}

	/**
	 * recursive if path is a directory and set to true, the directory is
	 * deleted else throws an exception. In case of a file the recursive can be
	 * set to either true or false.
	 * 
	 * @param fs
	 * @param path
	 * @param recursive
	 * @return
	 * @throws IOException
	 */
	public JSONObject delete(FileSystem fs, Path path, boolean recursive)
			throws IOException {
		JSONObject json = new JSONObject();
		boolean result = fs.delete(path, recursive);
		json.put("result", result);
		return json;
	}

	public JSONArray list(FileSystem fs, Path path, String baseUrl)
			throws IOException {
		FileStatus[] status = fs.listStatus(path);

		JSONArray json = new JSONArray();
		if (status != null) {
			for (FileStatus s : status) {
				json.add(FSUtils.fileStatusToJSON(s, baseUrl));
			}
		}
		return json;
	}

	public JSONObject put(FileSystem fs, Path distPath, InputStream is,
			boolean overwrite, int bufferSize) throws IOException {
		JSONObject json = new JSONObject();
		OutputStream os = fs.create(distPath, overwrite, bufferSize);
		InputStream in = new BufferedInputStream(is);
		IOUtils.copyBytes(in, os, bufferSize, true);
		json.put("result", true);
		return json;
	}
}
