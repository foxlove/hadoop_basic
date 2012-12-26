package org.hadoop.util;

public class ServerInfo {

	public static final String HDFS_BASE_URL = "hdfs://192.168.140.128:9000";
	public static final String WEB_DAV_BASE_URL = "http://192.168.140.128:8088";

	public static String getUserDefaultDirForHDFS(String userId) {
		return "/user/" + userId;// + "/";
	}
}
