package com.jerrylikecola.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author xiaxiang
 * @date 2020/6/11 4:34 下午
 */
public class Test {
    public static void main(String[] args) {
        String hostname = "你的数据库ip";
        Integer port = 11111;
        CanalConnector canalConnector = CanalConnectors.
                newSingleConnector(new InetSocketAddress(hostname,port),"example","","");
        int batchSize = 1000;
        int emptyCount = 0;
        try{
            canalConnector.connect();
            canalConnector.subscribe(".*\\..*");
            canalConnector.rollback();
            //close time
            int totalEmptyCount = 10000000;
            while(emptyCount < totalEmptyCount){
                //get data
                Message message = canalConnector.getWithoutAck(batchSize);
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0){
                    emptyCount++;
                    System.out.println("empty count :"+emptyCount);
                    try{
                        Thread.sleep(1000);
                    }catch (InterruptedException e){
                        System.out.println(e.getMessage());
                    }
                }else{
                    emptyCount = 0;
                    printEntry(message.getEntries());
                }
                canalConnector.ack(batchId);
            }
            System.out.println("exit");
        }finally {
            canalConnector.disconnect();
        }

    }

    /**
     * 打印方法
     * @param entrys
     */
    private static void printEntry(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }
            CanalEntry.EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));
            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == CanalEntry.EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList());
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList());
                } else {
                    System.out.println("-------> before");
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("-------> after");
                    printColumn(rowData.getAfterColumnsList());
                }
            }
        }
    }

    /**
     * 打印方法
     * @param columns
     */
    private static void printColumn(List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + " update=" + column.getUpdated());
        }
    }
}
