package org.example;


import org.example.Relations.SQLIterator;
import org.example.utils.JDBCUtil;
import org.example.utils.POJOFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;


public class TestQuery {
    public static void readTable() {
        String querySQL =
                "select\n" +
                "\tc_custkey,\n" +
                "\tc_name,\n" +
                "\tsum(l_extendedprice * (1 - l_discount)) as revenue,\n" +
                "\tc_acctbal,\n" +
                "\tn_name,\n" +
                "\tc_address,\n" +
                "\tc_phone,\n" +
                "\tc_comment\n" +
                "from\n" +
                "\tcustomer,\n" +
                "\torders,\n" +
                "\tlineitem,\n" +
                "\tnation\n" +
                "where\n" +
                "\tc_custkey = o_custkey\n" +
                "\tand l_orderkey = o_orderkey\n" +
                "\tand o_orderdate >= date '1992-10-01'\n" +
                "\tand o_orderdate < date '1993-1-01'\n" +
                "\tand l_returnflag = 'R'\n" +
                "\tand c_nationkey = n_nationkey\n" +
                "group by\n" +
                "\tc_custkey,\n" +
                "\tc_name,\n" +
                "\tc_acctbal,\n" +
                "\tc_phone,\n" +
                "\tn_name,\n" +
                "\tc_address,\n" +
                "\tc_comment\n" +
                "order by\n" +
                "\trevenue desc;\n";

        ResultSet rs = JDBCUtil.executeSQL(querySQL);
        try {
            while (rs.next()) {
//                int orderKey = rs.getInt(1); // 注意：索引从1开始
//                double revenue = rs.getDouble(2);
//                Date orderDate = rs.getDate(3);
//                String priority = rs.getString(4);
//                System.out.println("(" + orderKey + ", " + revenue + ", " + orderDate + " ," + priority + ")");

                int custKey = rs.getInt(1);
                String custName = rs.getString(2);
                double revenue = rs.getDouble(3);
                double custAcctBal = rs.getDouble(4);
                String nationName = rs.getString(5);
                String custAddress = rs.getString(6);
                String custPhone = rs.getString(7);
                String custComment = rs.getString(8);
                System.out.printf("(%s, %s, %2f, %2f, %s, %s, %s, %s)\n", custKey, custName, revenue, custAcctBal, nationName, custAddress, custPhone, custComment);


            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        JDBCUtil.truncateTable("customer");
        JDBCUtil.truncateTable("orders");
        JDBCUtil.truncateTable("lineItem");
        JDBCUtil.truncateTable("nation");

        Path path = Path.of("data/input_data_6000.csv");
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        int epoch = 1;

        for (String line : lines) {
            String[] fields = line.split("\\|");
            String operator = fields[0];
            String tag = fields[1];
            SQLIterator row = POJOFactory.of(tag, fields);
            if (operator.equals("+")) {
                JDBCUtil.insertIntoTable(tag, row.getKeys(), row.getValues());
            } else if (operator.equals("-")) {
                JDBCUtil.deleteFromTable(tag, Arrays.copyOfRange(row.getKeys(), 0, 2), Arrays.copyOfRange(row.getValues(), 0, 2));
            }
        }
        System.out.println("====================");
        readTable();
        JDBCUtil.close();
    }
}
