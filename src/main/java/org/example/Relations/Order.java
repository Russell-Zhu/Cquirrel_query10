package org.example.Relations;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Locale;

public class Order implements SQLIterator {
    public Boolean update;
    public int orderKey;
    public int custKey;
    public String orderStatus;
    public double totalPrice;
    public Date orderDate;
    public String orderPriority;
    public String clerk;
    public String shipPriority;
    public String comment;

    public String tag;

    public Order() {
    }

    public Order(Boolean update, int orderKey, int custKey, String orderStatus, double totalPrice, Date orderDate, String orderPriority, String clerk, String shipPriority, String comment, String tag) {
        this.update = update;
        this.orderKey = orderKey;
        this.custKey = custKey;
        this.orderStatus = orderStatus;
        this.totalPrice = totalPrice;
        this.orderDate = orderDate;
        this.orderPriority = orderPriority;
        this.clerk = clerk;
        this.shipPriority = shipPriority;
        this.comment = comment;
        this.tag = tag;
    }

    public Order(String[] fields) {
        if (fields.length < 11) {
            return;
        }
        int index=0;
        this.update = fields[index++].equals("+");
        this.tag = fields[index++];
        this.orderKey = Integer.parseInt(fields[index++]);
        this.custKey = Integer.parseInt(fields[index++]);
        this.orderStatus = fields[index++];
        this.totalPrice = Float.parseFloat(fields[index++]);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd",  Locale.US);
        try {
            this.orderDate = new Date(simpleDateFormat.parse(fields[index++]).getTime());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        this.orderPriority = fields[index++];
        this.clerk = fields[index++];
        this.shipPriority = fields[index++];
        this.comment = fields[index++];
    }

    @Override
    public String[] getKeys() {
        return new String[]{
                "O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDERDATE",
                "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT"
        };
    }

    @Override
    public Object[] getValues() {
        ArrayList<Object> values = new ArrayList<Object>();
        values.add(orderKey);
        values.add(custKey);
        values.add(orderStatus);
        values.add(totalPrice);
        values.add(orderDate);
        values.add(orderPriority);
        values.add(clerk);
        values.add(shipPriority);
        values.add(comment);
        return values.toArray();
    }

    @Override
    public String toString() {
        return "Order{" +
                "update=" + update +
                ", orderKey=" + orderKey +
                ", custKey=" + custKey +
                ", orderStatus='" + orderStatus + '\'' +
                ", totalPrice=" + totalPrice +
                ", orderDate=" + orderDate +
                ", orderPriority='" + orderPriority + '\'' +
                ", clerk='" + clerk + '\'' +
                ", shipPriority='" + shipPriority + '\'' +
                ", comment='" + comment + '\'' +
                ", tag='" + tag + '\'' +
                '}';
    }
}
