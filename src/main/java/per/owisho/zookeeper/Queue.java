package per.owisho.zookeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class Queue extends SyncPrimitive{

	Queue(String address,String name) throws KeeperException,InterruptedException, IOException{
		super(address);
		this.root = name;
		if(zk!=null){
			Stat s = zk.exists(root, false);
			if(s==null){
				zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		}
	}
	
	boolean produce(int i) throws KeeperException,InterruptedException{
		ByteBuffer b= ByteBuffer.allocate(4);
		byte[] value ;
		b.putInt(i);
		value = b.array();
		zk.create(root+"/elements",value,Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
		return true;
	}
	
	int consume() throws KeeperException,InterruptedException{
		Integer min = 0;
		int retvalue = -1;
		Stat stat = null;
		while(true){
			synchronized (mutex) {
				ArrayList<String> list = (ArrayList<String>) zk.getChildren(root, true);
				for(String s:list){
					Integer tempValue = new Integer(s.substring(8));
					if(tempValue < min) min = tempValue;
				}
				System.out.println("Temporary value:"+root+"/element"+min);
				byte[] b = zk.getData(root+"/element"+min,false, stat);
				zk.delete(root+"/element"+min, 0);
				ByteBuffer buffer = ByteBuffer.wrap(b);
				retvalue = buffer.getInt();
				return retvalue;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Queue queue = new Queue("localhost", "/localhost");
		queue.produce(10);
		System.out.println(queue.consume());
	}
	
}
