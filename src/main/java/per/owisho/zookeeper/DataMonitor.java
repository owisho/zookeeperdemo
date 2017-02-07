package per.owisho.zookeeper;

import java.util.Arrays;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class DataMonitor implements Watcher, StatCallback {

	ZooKeeper zk;

	String znode;

	Watcher chainedWatcher;

	boolean dead;

	DataMonitorListener listener;

	byte prevData[];

	public DataMonitor(ZooKeeper zk, String znode, Watcher chainedWatcher, DataMonitorListener listener) {
		this.zk = zk;
		this.znode = znode;
		this.chainedWatcher = chainedWatcher;
		this.listener = listener;
		zk.exists(znode, true, this, null);
	}

	public interface DataMonitorListener {
		/**
		 * The existence status of the node has changed
		 * 
		 * @param data
		 */
		void exists(byte data[]);

		/**
		 * The ZooKeeper session is no longer valid
		 * 
		 * @param rc
		 *            the Zookeeper reason code
		 */
		void closing(int rc);

	}

	@SuppressWarnings("deprecation")
	public void processResult(int rc, String path, Object ctx, Stat stat) {
		boolean exists;
		switch (rc) {
		case Code.Ok:
			exists = true;
			break;
		case Code.NoNode:
			exists = false;
			break;
		case Code.SessionExpired:
		case Code.NoAuth:
			dead = true;
			listener.closing(rc);
			return;
		default:
			// Retry errors
			zk.exists(znode, true, this, null);
			return;
		}

		byte b[] = null;
		if (exists) {
			try {
				b = zk.getData(znode, false, null);
			} catch (KeeperException e) {
				// We don't need to worry about recovering now. The watch
				// callbacks will kick off any exception handling
				e.printStackTrace();
			} catch (InterruptedException e) {
				return;
			}
		}
		if ((b == null && b != prevData) || (b != null && !Arrays.equals(prevData, b))) {
			listener.exists(b);
			prevData = b;
		}
	}

	@SuppressWarnings("deprecation")
	public void process(WatchedEvent event) {
		String path = event.getPath();
		if (event.getType() == Event.EventType.None) {
			// We are are being told that the state of the
			// connection has changed
			switch (event.getState()) {
			case SyncConnected:
				// In this particular example we don't need to do anything
				// here - watches are automatically re-registered with
				// server and any watches triggered while the client was
				// disconnected will be delivered (in order of course)
				break;
			case Expired:
				// It's all over
				dead = true;
				listener.closing(KeeperException.Code.SessionExpired);
				break;
			default:
				break;
			}
		} else {
			if (path != null && path.equals(znode)) {
				// Something has changed on the node, let's find out
				zk.exists(znode, true, this, null);
			}
		}
		if (chainedWatcher != null) {
			chainedWatcher.process(event);
		}

	}

}
