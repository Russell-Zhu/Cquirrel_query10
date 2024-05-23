package org.example.Relations;

import java.util.ArrayList;

/**
 * Project name: CquirrelDemo
 * Class name：Customer
 * Description：TODO
 * Create time：2023/2/1 21:11
 * Creator：ellilachen
 */
public class Customer implements SQLIterator{
    public Boolean update;
    public int custKey;
    public String name;
    public String address;
    public int nationKey;
    public String phone;
    public float acctbal;
    public String mktsegment;
    public String comment;

    public String tag;

    public Customer() {}

    public Customer(Boolean update, int custKey, String name, String nationKey, String phone, float acctbal, String mktsegment, String comment, String tag) {
        this.update = update;
        this.custKey = custKey;
        this.name = name;
        this.nationKey = Integer.parseInt(nationKey);
        this.phone = phone;
        this.acctbal = acctbal;
        this.mktsegment = mktsegment;
        this.comment = comment;
        this.tag = tag;
    }

    public Customer(String[] fields) {
        if (fields.length < 10) {
            return;
        }
        int index=0;
        this.update = fields[index++].equals("+");
        this.tag = fields[index++];
        this.custKey = Integer.parseInt(fields[index++]);
        this.name = fields[index++];
        this.address = fields[index++];
        this.nationKey = Integer.parseInt(fields[index++]);
        this.phone = fields[index++];
        this.acctbal = Float.parseFloat(fields[index++]);
        this.mktsegment = fields[index++];
        this.comment = fields[index++];
    }

    @Override
    public String[] getKeys() {
        return new String[] {"C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT"};
    }


    @Override
    public Object[] getValues() {
        ArrayList<Object> values = new ArrayList<>();
        values.add(this.custKey);
        values.add(this.name);
        values.add(this.address);
        values.add(this.nationKey);
        values.add(this.phone);
        values.add(this.acctbal);
        values.add(this.mktsegment);
        values.add(this.comment);
        return values.toArray();
    }

    @Override
    public String toString() {
        return "Customer{" +
                "update=" + update +
                ", custKey=" + custKey +
                ", name='" + name + '\'' +
                ", address='" + address + '\'' +
                ", nationKey='" + nationKey + '\'' +
                ", phone='" + phone + '\'' +
                ", acctbal=" + acctbal +
                ", mktsegment='" + mktsegment + '\'' +
                ", comment='" + comment + '\'' +
                ", tag='" + tag + '\'' +
                '}';
    }
}
