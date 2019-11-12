package com.atguigu.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HdfsClient {
	
	private Configuration conf ;
	private FileSystem fs;
	
	/**
	 * 需求: 从HDFS 分块 下载 hadoop-2.7.2.tar.gz
	 * 
	 * 第一次下载:  0~128M
	 * 第二次下载:  128 ~ 
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * 
	 */
	@Test
	public void testBlockDownload1() throws IllegalArgumentException, IOException {
		//输入流
		FSDataInputStream in = 
					fs.open(new Path("/user/atguigu/input/hadoop-2.7.2.tar.gz"));
		
		//输出流
		FileOutputStream out = 
					new FileOutputStream(new File("d:/source/hadoop-2.7.2.tar.gz.part1"));
		
		//流的对拷
		byte [] buffer = new byte[1024];
		
		for(int i =0 ; i<1024*128; i++) {
			in.read(buffer);
			out.write(buffer);
		}
		
		//关闭
		out.close();
		in.close();
	}
	
	@Test
	public void testBlockDownload2() throws IllegalArgumentException, IOException {
		//输入流
		FSDataInputStream in = 
					fs.open(new Path("/user/atguigu/input/hadoop-2.7.2.tar.gz"));
		
		//输出流
		FileOutputStream out = 
					new FileOutputStream(new File("d:/source/hadoop-2.7.2.tar.gz.part2"));
		
		//流的对拷
		in.seek(1024*1024*128);
		
		IOUtils.copyBytes(in, out, conf);
		
		//关闭
		out.close();
		in.close();
	}
	
	
	
	
	/**
	 * IO流操作: 文件下载
	 */
	
	@Test
	public void testIOFileDownload()throws Exception {
		//1.输入流
		FSDataInputStream in = fs.open(new Path("/wc.txt"));
		
		//2.输出流
		FileOutputStream out = new FileOutputStream(new File("d:/source/wc.txt"));
		
		//3. 流的对拷
		IOUtils.copyBytes(in, out, conf);
		//4. 关闭
		out.close();
		
		in.close();
		
	}
	
	/**
	 * IO流操作: 文件上传
	 */
	@Test
	public void testIOFileUpload() throws Exception {
		//1. 输入流
		FileInputStream in = new FileInputStream(new File("d:/source/xiaochichi.txt"));
		//2. 输出流
		FSDataOutputStream out = fs.create(new Path("/user/atguigu/input/xiaochichi.txt"));
		//3. 流的对拷
		IOUtils.copyBytes(in, out, conf);
		
		//4. 关闭
		out.close();
		in.close();
		
		
		/*byte [] buffer = new byte[1024];
		int i ;
		while((i = in.read(buffer))!=-1) {
			out.write(buffer, 0, i);
		}*/
		
	}
	
	
	
	
	
	
	/**
	 * 需求: 传入一个path，显示该path下所有的文件和目录(包含子目录)
	 * 	    File: xxxxx
	 * 		Dir: /xx/xxx 
	 */
	@Test
	public void testListFileAndDir()  throws Exception{
		listFileAndDir(new Path("/"), fs);
	}
	
	public void  listFileAndDir(Path path ,  FileSystem fs ) throws Exception {
		FileStatus[] listStatus = fs.listStatus(path);
		for (FileStatus fileStatus : listStatus) {
			
			if(fileStatus.isFile()) {
				//文件
				if(path.toString().equals("/")) {
					System.out.println("File:" +"/" +fileStatus.getPath().getName());
				}else {
					System.out.println("File:" + path.toString() + "/" +fileStatus.getPath().getName());
				}
			}else {
				//目录
				//fileStatus.getPath(): hdfs://hadoop102:9000/user
				String dir = fileStatus.getPath().toString().substring("hdfs://hadoop102:9000".length());
				System.out.println("Dir: "+ dir );
				
				listFileAndDir(new Path(dir), fs);
			}
		}
	}
	
	
	
	/**
	 * 文件和目录的判断
	 */
	@Test
	public void testListStatus() throws Exception {
		
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus) {
			if(fileStatus.isFile()) {
				System.out.println(fileStatus.getPath().getName() + "  是文件");
			}else {
				System.out.println(fileStatus.getPath().getName() +"   是目录");
			}
		}
	}
	
	
	
	
	
	
	
	/**
	 * 文件详情查看
	 */
	@Test
	public void testListFiles() throws Exception {
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		//迭代
		while(listFiles.hasNext()) {
			LocatedFileStatus next = listFiles.next();
			System.out.println(next.getBlockSize());
			System.out.println(next.getLen());
			System.out.println(next.getPath());
			System.out.println(next.getReplication());
			BlockLocation[] blockLocations = next.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations) {
				System.out.println(blockLocation.getLength());
				System.out.println(Arrays.toString(blockLocation.getHosts()));
			}
			
			System.out.println("=============================================");
		}
		
		
	}
	
	/**
	 * 文件 和 目录的更名
	 */
	@Test
	public void testRename() throws Exception{
		fs.rename(new Path("/wc.input"), new Path("/wc.txt"));
	}
	
	
	
	/**
	 * 文件夹 和 文件 删除
	 */
	@Test
	public void testDelete()  throws Exception{
		fs.delete(new Path("/bigdata0826"), false);
	}
	
	
	
	
	/**
	 * 文件下载
	 */
	@Test
	public void testCopyToLocalFile() throws Exception{
		fs.copyToLocalFile(false, new Path("/wc.input"), new Path("d:/source"), true);
	}
	
	
	/**
	 * 配置文件优先级: 
	 * Configuration.set()  > 当前项目中的配置文件  >  xxx-site.xml   >  xxx-default.xml
	 */
	
	/**
	 * 文件上传
	 */
	@Test
	public void testCopyFromLocal() throws Exception {
		fs.copyFromLocalFile(new Path("d:"+File.separator+"source"+File.separator+"songsong.txt"), 
									new Path("/user/atguigu/input/bigdata0826"));
		
		// File.seperator
	}
	
	/**
	 *  @Before注解标注的方法会在每个@Test注解标注的方法之前执行.
	 */
	@Before
	public void  before() throws Exception{
		conf = new Configuration();
		//conf.set("dfs.replication", "2");
		fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"atguigu");
	}
	
	/**
	 * @After注解标注的方法会在每个@Test注解标注的方法之后执行.
	 */
	@After
	public void  after() throws Exception{
		fs.close();
	}
	/**
	 * 测试FileSystem
	 */
	@Test
	public void testFileSystem() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000") ,conf ,"atguigu");
		System.out.println("fs:" + fs );
		
		//在hdfs上创建一个目录
		fs.mkdirs(new Path("/user/atguigu/input/bigdata0826"));
		
		fs.close();
	}
}
