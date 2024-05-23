package org.example.Relations;

import java.util.ArrayList;
import java.util.List;


public class Nation implements SQLIterator {
    public Boolean update;
    public int nationKey;
    public String name;
    public int regionKey;
    public String comment;
    public String tag;

    public Nation() {}

    public Nation(Boolean update, int nationKey, String name, int regionKey, String comment, String tag) {
        this.update = update;
        this.nationKey = nationKey;
        this.name = name;
        this.regionKey = regionKey;
        this.comment = comment;
        this.tag = tag;
    }

    public Nation(String[] fields) {
        if (fields.length < 6) {
            return;
        }
        int index=0;
        this.update = fields[index++].equals("+");
        this.tag = fields[index++];
        this.nationKey = Integer.parseInt(fields[index++]);
        this.name = fields[index++];
        this.regionKey = Integer.parseInt(fields[index++]);
        this.comment = fields[index++];

    }

    @Override
    public String[] getKeys() {
        return new String[] {
                "N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"
        };
    }

    @Override
    public Object[] getValues() {
        List<Object> values = new ArrayList<>();
        values.add(this.nationKey);
        values.add(this.name);
        values.add(this.regionKey);
        values.add(this.comment);
        return values.toArray();
    }

    @Override
    public String toString() {
        return "Nation{" +
                "update=" + update +
                ", nationKey=" + nationKey +
                ", name='" + name + '\'' +
                ", regionKey=" + regionKey +
                ", comment='" + comment + '\'' +
                ", tag='" + tag + '\'' +
                '}';
    }
}
