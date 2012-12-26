package org.hadoop.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class FSFactory {

	public static FileSystem getInstance() throws IOException {
		return FileSystem.get(URI.create(ServerInfo.HDFS_BASE_URL),
				new Configuration());
	}

	public static FileSystem getInstance(URI uri, Configuration conf)
			throws IOException {
		return FileSystem.get(uri, conf);
	}

	public static FileSystem getInstance(Configuration conf) throws IOException {
		return FileSystem.get(conf);
	}

}
