package per.owisho.zookeeper;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

//既作为了watcher又作为了datamonitorlistener
//作为watcher 实现了process接口，zookeeper使用process接口与通用事件（主线程感兴趣的zookeeper连接状态和zookeeper会话状态）沟通
//作为datamonitorlistener是为了让他所包含的datamonitor对象与他进行沟通
public class Executor implements Watcher,Runnable,DataMonitor.DataMonitorListener{
	
	public static void main(String[] args) {
		if(args.length<4){
			System.err.println("USAGE: Executor hostPort znode filename program [args ...]");
			System.exit(2);
		}
		String hostPort = args[0];
		String znode = args[1];
		String filename = args[2];
		String exec[] = new String[args.length-3];
		System.arraycopy(args, 3, exec, 0, exec.length);
		
		try {
			new Executor(hostPort, znode, filename, exec).run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	String znode;
	
	//监控数据
	DataMonitor dm;
	
	//维持连接
	ZooKeeper zk;
	
	String filename;
	
	String exec[];
	
	Process child;
	
	public Executor(String hostPort,String znode,String filename,String exec[]) throws KeeperException,IOException{
		this.filename = filename;
		this.exec = exec;
		zk = new ZooKeeper(hostPort, 3000, this);
		dm = new DataMonitor(zk,znode,null,this);
	}

	public void run() {
		try {
			synchronized (this) {
				while(!dm.dead){
					wait();
				}
			}
		} catch (Exception e) {
		}
	}

	public void process(WatchedEvent event) {
		dm.process(event);
	}

	public void exists(byte[] data) {
		
		if(data==null){
			if(child!=null){
				System.out.println("Killing process");
				child.destroy();
				try {
					child.waitFor();
				} catch (Exception e) {
				}
			}
		}else{
			if(child!=null){
				System.out.println("Stopping child");
				child.destroy();
				try {
					child.waitFor();
				} catch (Exception e) {
				}
			}
			try {
				FileOutputStream fos = new FileOutputStream(filename);
				fos.write(data);
				fos.close();
			} catch (Exception e) {
			}
			
			try {
				System.out.println("Starting child");
				child = Runtime.getRuntime().exec(exec);
				new StreamWriter(child.getInputStream(),System.out);
				new StreamWriter(child.getErrorStream(),System.out);
			} catch (Exception e) {
			}
		}
		
	}

	public void closing(int rc) {
		synchronized (this) {
			notifyAll();
		}
	}
	
	static class StreamWriter extends Thread{
		
		OutputStream os;
		
		InputStream in;
		
		public StreamWriter(InputStream in,OutputStream os) {
			this.os = os;
			this.in = in;
			start();
		}
		
		@Override
		public void run() {
			byte[] b = new byte[80];
			int rc;
			try {
				while ((rc = in.read(b))>0) {
					os.write(b, 0, rc);
				}
			} catch (Exception e) {
			}
		}
		
	}
	
}
