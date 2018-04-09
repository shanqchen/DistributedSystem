package ds.demo.zookeeper.prime;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

//ref: https://blog.csdn.net/haoyuyang/article/details/53436625
public class ZookeeperBase {

    static final String CONNECT_ADDR = "10.0.112.95:2181";
    
    static final int SESSION_OUTTIME = 2000; //ms
    
    /** 信号量，阻塞程序执行，用于等待zookeeper连接成功，发送成功信号 */ 
    static final CountDownLatch connectedSemaphore = new CountDownLatch(1);
    
    public static void main(String[] args) throws Exception {
        ZooKeeper zk = new ZooKeeper(CONNECT_ADDR, SESSION_OUTTIME, new Watcher(){

            @Override
            public void process(WatchedEvent event) {
                // TODO Auto-generated method stub
                KeeperState keeperState = event.getState();
                EventType eventType = event.getType();
                //
                if(KeeperState.SyncConnected == keeperState){
                    if(EventType.None == eventType){
                        //如果建立连接成功，则发送信号量，让后续阻塞程序向下执行
                        connectedSemaphore.countDown();
                        System.out.println("zk建立连接");
                    }
                }
            }
            
        });
            
        //进行阻塞
        connectedSemaphore.await();
        
        System.out.println("..");
        
        //创建节点
//        zk.create("/testRoot", "testRoot".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
        zk.create("/testRoot/children", "children data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        
        //获取节点信息
        try{
            byte[] data = zk.getData("/testRoot", false, null);
            System.out.println(new String(data));
        }catch(Exception e){
            e.printStackTrace();
        }
        
        //修改节点的值，-1表示跳过版本检查，其他正数表示如果传入的版本号与当前版本号不一致，则修改不成功，删除是同样的道理。  
        zk.setData("/testRoot", "modify data root".getBytes(), -1);
        
        byte[] data2 = zk.getData("/testRoot", false, null);
        System.out.println(new String(data2));
        
        //判断节点是否存在
        System.out.println(zk.exists("/testRoot/children", false));
        
        //删除节点
        zk.delete("/testRoot/children", -1);
        System.out.println(zk.exists("/testRoot/children", false));
        
        zk.close();
    }
}
