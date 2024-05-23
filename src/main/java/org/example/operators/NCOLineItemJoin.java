package org.example.operators;



import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.example.Relations.LineItem;
import org.example.Relations.Order;

import java.sql.Date;
// left: update, customer.custKey, customer.name, customer.acctbal, nation.name, customer.address, customer.phone, customer.comment, order.orderKey, order.orderDate
// Tuple10<Boolean, Integer, String, Float, String, String, String, String, Integer, Date>

// right: LineItem

// return: update, customer.custKey, customer.name, customer.acctbal, nation.name, customer.address, customer.phone, customer.comment, lineitem.extendedPrice, lineitem.discount, lineitem.returnFlag
// update, left.f1, left.f2, left.f3, left.f4, left.f5, left.f6, left.f7, lineItem.extendedPrice, lineItem.discount, lineItem.returnFlag
// Tuple11<Boolean, Integer, String, Float, String, String, String, String, Double, Double, String>
public class NCOLineItemJoin extends CoProcessFunction<
        Tuple10<Boolean, Integer, String, Float, String, String, String, String, Integer, Date>,
        LineItem,
        Tuple11<Boolean, Integer, String, Float, String, String, String, String, Double, Double, String>> {

    private ValueState<Tuple10<Boolean, Integer, String, Float, String, String, String, String, Integer, Date>> leftState;
    private ListState<LineItem> lineItemListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        leftState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("left", Types.TUPLE(Types.BOOLEAN, Types.INT, Types.STRING, Types.FLOAT, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.SQL_DATE)));
        lineItemListState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("lineitem", Types.POJO(LineItem.class)));
    }

    @Override
    public void processElement1(Tuple10<Boolean, Integer, String, Float, String, String, String, String, Integer, Date> left,
                                CoProcessFunction<
                                        Tuple10<Boolean, Integer, String, Float, String, String, String, String, Integer, Date>,
                                        LineItem,
                                        Tuple11<Boolean, Integer, String, Float, String, String, String, String, Double, Double, String>>.Context context,
                                Collector<Tuple11<Boolean, Integer, String, Float, String, String, String, String, Double, Double, String>> collector) throws Exception {
        leftState.update(left);
        for (LineItem lineItem : lineItemListState.get()) {
            Boolean update = left.f0 && lineItem.update;
            Tuple11<Boolean, Integer, String, Float, String, String, String, String, Double, Double, String> tuple
                    = Tuple11.of(update, left.f1, left.f2, left.f3, left.f4, left.f5, left.f6, left.f7, lineItem.extendedPrice, lineItem.discount, lineItem.returnFlag);
            if (update || (left.f0 ^ lineItem.update)) {
                collector.collect(tuple);
            }
        }
    }

    @Override
    public void processElement2(LineItem lineItem,
                                CoProcessFunction<
                                        Tuple10<Boolean, Integer, String, Float, String, String, String, String, Integer, Date>,
                                        LineItem,
                                        Tuple11<Boolean, Integer, String, Float, String, String, String, String, Double, Double, String>>.Context context,
                                Collector<Tuple11<Boolean, Integer, String, Float, String, String, String, String, Double, Double, String>> collector) throws Exception {
        lineItemListState.add(lineItem);
        Tuple10<Boolean, Integer, String, Float, String, String, String, String, Integer, Date> left = leftState.value();
        if (left != null) {
            Boolean update = lineItem.update && left.f0;
            Tuple11<Boolean, Integer, String, Float, String, String, String, String, Double, Double, String> tuple
                    = Tuple11.of(update, left.f1, left.f2, left.f3, left.f4, left.f5, left.f6, left.f7, lineItem.extendedPrice, lineItem.discount, lineItem.returnFlag);
            if (update || (left.f0 ^ lineItem.update)) {
                collector.collect(tuple);
            }
        }
    }
}
