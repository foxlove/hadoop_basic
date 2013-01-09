package org.hadoop.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class FSFactory {

	private static FileSystem FS = null;
	private static Configuration conf = new Configuration();

	public FSFactory() throws IOException {
		FS = FileSystem.get(URI.create(ServerInfo.HDFS_BASE_URL), conf);
	}

	public FSFactory(URI uri, Configuration conf) throws IOException {
		FS = FileSystem.get(uri, conf);
	}

	public FSFactory(Configuration conf) throws IOException {
		FS = FileSystem.get(conf);
	}

	public FileSystem getInstance() throws IOException {
		return FS;
	}
}
