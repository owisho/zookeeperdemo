package per.owisho.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class SyncPrimitive implements Watcher{

	static ZooKeeper zk = null;
	static final Object mutex = new Object();
	String root;
	
	SyncPrimitive(String address) throws KeeperException,IOException{
		if(zk==null){
			System.out.println("Starting ZK:");
			zk = new ZooKeeper(address,3000,this);
			System.out.println("Finished starting ZK:"+zk);
		}
	}
	
	public void process(WatchedEvent event) {
		synchronized (mutex) {
			mutex.notify();
		}
	}

}
