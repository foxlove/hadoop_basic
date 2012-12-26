package org.hadoop.util;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class FSUtils {
	/**
	 * Constant for the default permission string ('default').
	 */
	public static final String DEFAULT_PERMISSION = "default";

	/**
	 * Converts a Unix permission symbolic representation (i.e. -rwxr--r--) into
	 * a Hadoop permission.
	 * 
	 * @param str
	 *            Unix permission symbolic representation.
	 * @return the Hadoop permission. If the given string was 'default', it
	 *         returns <code>FsPermission.getDefault()</code>.
	 */
	public static FsPermission getPermission(String str) {
		FsPermission permission;
		if (str.equals(DEFAULT_PERMISSION)) {
			permission = FsPermission.getDefault();
		} else {
			// TODO: there is something funky here, it does not detect 'x'
			permission = FsPermission.valueOf(str);
		}
		return permission;
	}

	/**
	 * Replaces the <code>SCHEME://HOST:PORT</code> of a Hadoop
	 * <code>Path</code> with the specified base URL.
	 * 
	 * @param path
	 *            Hadoop path to replace the <code>SCHEME://HOST:PORT</code>.
	 * @param hoopBaseUrl
	 *            base URL to replace it with.
	 * @return the path using the given base URL.
	 */
	public static Path convertPathToHoop(Path path, String baseUrl) {
		URI uri = path.toUri();
		String filePath = uri.getRawPath();
		String p = baseUrl + filePath;
		return new Path(p);
	}

	/**
	 * Converts a Hadoop permission into a Unix permission symbolic
	 * representation (i.e. -rwxr--r--) or default if the permission is NULL.
	 * 
	 * @param p
	 *            Hadoop permission.
	 * @return the Unix permission symbolic representation or default if the
	 *         permission is NULL.
	 */
	private static String permissionToString(FsPermission p) {
		return (p == null) ? "default" : "-" + p.getUserAction().SYMBOL
				+ p.getGroupAction().SYMBOL + p.getOtherAction().SYMBOL;
	}

	/**
	 * Converts a Hadoop <code>FileStatus</code> object into a JSON array
	 * object. It replaces the <code>SCHEME://HOST:PORT</code> of the path with
	 * the specified URL.
	 * <p/>
	 * 
	 * @param status
	 *            Hadoop file status.
	 * @param baseUrl
	 *            base URL to replace the <code>SCHEME://HOST:PORT</code> in the
	 *            file status.
	 * @return The JSON representation of the file status.
	 */
	@SuppressWarnings("unchecked")
	public static Map fileStatusToJSON(FileStatus status, String baseUrl) {
		Map json = new LinkedHashMap();
		json.put("path", convertPathToHoop(status.getPath(), baseUrl)
				.toString());
		json.put("isDir", status.isDir());
		json.put("len", status.getLen());
		json.put("owner", status.getOwner());
		json.put("group", status.getGroup());
		json.put("permission", permissionToString(status.getPermission()));
		json.put("accessTime", status.getAccessTime());
		json.put("modificationTime", status.getModificationTime());
		json.put("blockSize", status.getBlockSize());
		json.put("replication", status.getReplication());
		return json;
	}

	/**
	 * Converts a Hadoop <code>FileStatus</code> array into a JSON array object.
	 * It replaces the <code>SCHEME://HOST:PORT</code> of the path with the
	 * specified URL.
	 * <p/>
	 * 
	 * @param status
	 *            Hadoop file status array.
	 * @param hoopBaseUrl
	 *            base URL to replace the <code>SCHEME://HOST:PORT</code> in the
	 *            file status.
	 * @return The JSON representation of the file status array.
	 */
	@SuppressWarnings("unchecked")
	public static JSONArray fileStatusToJSON(FileStatus[] status,
			String hoopBaseUrl) {
		JSONArray json = new JSONArray();
		if (status != null) {
			for (FileStatus s : status) {
				json.add(fileStatusToJSON(s, hoopBaseUrl));
			}
		}
		return json;
	}

	/**
	 * Converts an object into a Json Map with with one key-value entry.
	 * <p/>
	 * It assumes the given value is either a JSON primitive type or a
	 * <code>JsonAware</code> instance.
	 * 
	 * @param name
	 *            name for the key of the entry.
	 * @param value
	 *            for the value of the entry.
	 * @return the JSON representation of the key-value pair.
	 */
	@SuppressWarnings("unchecked")
	public static JSONObject toJSON(String name, Object value) {
		JSONObject json = new JSONObject();
		json.put(name, value);
		return json;
	}
}
