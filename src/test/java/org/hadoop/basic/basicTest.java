package org.hadoop.basic;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.hadoop.util.FSFactory;
import org.hadoop.util.Hadoop;
import org.hadoop.util.ServerInfo;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class basicTest {

	private final static Logger logger = LoggerFactory
			.getLogger(basicTest.class);

	private Hadoop hadoop;
	private FSFactory fsFactory;

	@Before
	public void setup() {
		// for HDFS call
		hadoop = new Hadoop();
		try {
			fsFactory = new FSFactory();
		} catch (IOException e) {
			logger.error(" ####################FSFactory create instance FAIL !!!");
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@After
	public void tearDown(){
		hadoop = null;
		fsFactory = null;
	}

	@Test
	public void HDFS_getHomeDir() {
		try {
			JSONObject jsObj = hadoop.getHomeDir(fsFactory.getInstance());
			logger.debug("jsonObj : {}", jsObj.toJSONString());
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void HDFS_mkDir() {

		Path path = new Path("/user/root/mkDirTest");

		try {
			JSONObject jsObj = hadoop.makeDir(fsFactory.getInstance(), path);
			logger.debug("jsonObj : {}", jsObj.toJSONString());
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void HDFS_delDir() {

		Path path = new Path("/user/root/mkDirTest");

		try {
			JSONObject jsObj = hadoop.delete(fsFactory.getInstance(), path,
					true);
			logger.debug("jsonObj : {}", jsObj.toJSONString());
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void HDFS_list() {

		String distDir = "/user/root/output/";
		Path path = new Path(distDir);
		JSONArray jsArray = null;

		try {
			jsArray = hadoop.list(fsFactory.getInstance(), path,
					ServerInfo.HDFS_BASE_URL);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}

		logger.debug("jsArray : {}", jsArray.toJSONString());

		Iterator<JSONObject> itr = jsArray.iterator();
		String fileName = "";
		Object jsObj;

		while (itr.hasNext()) {
			jsObj = itr.next();
			fileName = ((HashMap) jsObj).get("path").toString()
					.replace(ServerInfo.HDFS_BASE_URL + distDir, "");
			logger.debug("fileName : {}", fileName);
		}

	}

	@Test
	public void HDFS_put() {

		String inputDir = "/usr/local/eclipse/";
		String distDir = "/user/root/output/";

		String fileName = "chiwoo-codestyle.xml";
		Path path = new Path(distDir + fileName);
		InputStream is = null;

		try {
			is = new BufferedInputStream(new FileInputStream(inputDir
					+ fileName));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
			Assert.fail();
		}

		try {
			JSONObject jsObj = hadoop.put(fsFactory.getInstance(), path, is,
					true, 1024);
			logger.debug("jsonObj : {}", jsObj.toJSONString());
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}

		HDFS_list();
	}

	@Test
	public void HDFS_get() {
		Path path = new Path(ServerInfo.HDFS_BASE_URL
				+ "/user/root/output/movie.mp4");
		try {
			DataInputStream fsdis = hadoop.get(fsFactory.getInstance(), path);
			logger.debug(">> size : {} MB", fsdis.available() / (1024L * 1024L));
			
			OutputStream baos = new FileOutputStream("/root/Desktop/hadoop_copy_mv.mp4");
			
			byte[] buf = new byte[1024];
			int readCnt = 0;
			
			while((readCnt = fsdis.read(buf)) != -1){
				baos.write(buf, 0, readCnt);
			}
			baos.flush();
			
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void general_file_read(){
		try {
			InputStream fis = new FileInputStream("/root/Desktop/movie.mp4");
			logger.debug(">> size : {} MB", fis.available() / (1024L * 1024L));
			
			OutputStream baos = new FileOutputStream("/root/Desktop/general_copy_mv.mp4");
			
			byte[] buf = new byte[1024];
			int readCnt = 0;
			
			while((readCnt = fis.read(buf)) != -1){
				baos.write(buf, 0, readCnt);
			}
			baos.flush();

		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void HDFS_delFile() {

		Path path = new Path("/user/root/output/chiwoo-codestyle.xml");

		try {
			JSONObject jsObj = hadoop.delete(fsFactory.getInstance(), path,
					true);
			logger.debug("jsonObj : {}", jsObj.toJSONString());
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}

		HDFS_list();
	}

	// ===============================

	@Test
	public void readFile() {
		try {
			String[] args = { "hdfs://192.168.140.128:9000/user/root/input/mapred-site.xml" };

			URLCat_readFile.main(args);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void FileCopy() {
		try {
			String[] args = { "/usr/local/hadoop-0.20.1/build.xml",
					"hdfs://192.168.140.128:9000/user/root/output/copy_build.xml" };

			String[] output = { "hdfs://192.168.140.128:9000/user/root/output/" };

			FileCopyWithProgress.main(args);

			logger.debug("========  shell command ============ ");
			// callShellCommand(
			// "/usr/local/hadoop-0.20.1/bin/hadoop fs -ls output/", true );

			logger.debug("======== List FileStatus ============ ");
			FileListStatus.main(output);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	private void callShellCommand(String command, boolean display)
			throws Exception {
		java.lang.Runtime runTime = java.lang.Runtime.getRuntime();
		java.lang.Process process = runTime.exec(command);

		System.out.println(process.waitFor());

		if (display) {
			IOUtils.copyBytes(process.getInputStream(), System.out, 4096, false);
		}
	}
}
