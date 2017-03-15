import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

public class SuperTable {

    public static void main(String[] args) throws IOException {

        // Instantiating configuration class
        Configuration conf = HBaseConfiguration.create();

        // Instantiating HTable class
        createTable(conf);

        // Repeat these steps as many times as necessary

        // Instantiating Put class
        // Hint: Accepts a row name

        // Add values using add() method
        // Hints: Accepts column family name, qualifier/row name ,value

        // Save the table

        // Close table

        // Instantiate the Scan class

        // Scan the required columns

        // Get the scan result

        // Read values from scan result
        // Print scan result

        // Close the scanner

        // Htable closer
        doWork(conf);
    }


    private static void createTable(Configuration conf) {
        // Instantiating HbaseAdmin class
        HBaseAdmin admin = new HBaseAdmin(conf);

        // Instantiating table descriptor class
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("powers"));

        // Adding column families to table descriptor
        tableDescriptor.addFamily(new HColumnDescriptor("personal"));
        tableDescriptor.addFamily(new HColumnDescriptor("professional"));

        // Execute the table through admin
        admin.createTable(tableDescriptor);
        System.out.println(" Table created ");

    }

    private static void doWork(Configuration conf) {
        // Instantiating HTable class
        HTable hTable = new HTable(conf, "powers");

        // Instantiating Put class
        // accepts a row name.
        Put p1 = new Put(Bytes.toBytes("row1"));

        // adding values using add() method
        // accepts column family name, qualifier/row name ,value
        p1.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes("superman"));
        p1.add(Bytes.toBytes("personal"), Bytes.toBytes("power"), Bytes.toBytes("strength"));
        p1.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes("clark"));
        p1.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes("100"));

        // Instantiating Put class
        // accepts a row name.
        Put p2 = new Put(Bytes.toBytes("row2"));

        // adding values using add() method
        // accepts column family name, qualifier/row name ,value
        p2.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes("batman"));
        p2.add(Bytes.toBytes("personal"), Bytes.toBytes("power"), Bytes.toBytes("money"));
        p2.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes("bruce"));
        p2.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes("50"));

        // Instantiating Put class
        // accepts a row name.
        Put p3 = new Put(Bytes.toBytes("row3"));

        // adding values using add() method
        // accepts column family name, qualifier/row name ,value
        p3.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes("wolverine"));
        p3.add(Bytes.toBytes("personal"), Bytes.toBytes("power"), Bytes.toBytes("healing"));
        p3.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes("logan"));
        p3.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes("75"));


        // Saving the put Instance to the HTable.
        hTable.put(p1);
        hTable.put(p2);
        hTable.put(p3);

        // closing HTable
        hTable.close();

        // Instantiating HTable class
        HTable table = new HTable(conf, "powers");

        // Instantiating the Scan class
        Scan scan = new Scan();

        // Scanning the required columns
        scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("hero"));

        // Getting the scan result
        ResultScanner scanner = table.getScanner(scan);

        // Reading values from scan result
        for (Result result = scanner.next(); result != null; result = scanner.next())

            System.out.println("" + result);
        //closing the scanner
        scanner.close();

    }
}