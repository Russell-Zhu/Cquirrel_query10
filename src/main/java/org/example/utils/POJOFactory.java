package org.example.utils;


import org.example.Relations.*;
import org.example.Tables;

import java.security.InvalidParameterException;


public class POJOFactory {
    static public SQLIterator of(String tag, String[] fields) {
        if (fields.length > 1) {
            if (tag.equals(Tables.customer.name())) {
                return new Customer(fields);
            } else if (tag.equals(Tables.orders.name())) {
                return new Order(fields);
            } else if (tag.equals(Tables.lineitem.name())) {
                return new LineItem(fields);
            } else if (tag.equals(Tables.nation.name())) {
                return new Nation(fields);
            }else {
                throw new InvalidParameterException("Do not support this kind of tag");
            }
        } else {
            throw new InvalidParameterException("There are no enough fields");
        }
    }
}
