package org.example.operators;


import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.example.Relations.Customer;
import org.example.Relations.Order;

import java.sql.Date;

// left: update, customer.custKey, customer.name, customer.acctbal, nation.name, customer.address, customer.phone, customer.comment
// Tuple8<Boolean, Integer, String, Float, String, String, String, String>

// right: order
// return: update, customer.custKey, customer.name, customer.acctbal, nation.name, customer.address, customer.phone, customer.comment, order.orderKey, order.orderDate
// Tuple10<Boolean, Integer, String, Float, String, String, String, String, Integer, Date>
public class NCOrderJoin extends CoProcessFunction<
        Tuple8<Boolean, Integer, String, Float, String, String, String, String>,
        Order,
        Tuple10<Boolean, Integer, String, Float, String, String, String, String, Integer, Date>> {

    private ValueState<Tuple8<Boolean, Integer, String, Float, String, String, String, String>> leftState;
    private ListState<Order> orderListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        leftState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("left", Types.TUPLE(Types.BOOLEAN, Types.INT, Types.STRING, Types.FLOAT, Types.STRING, Types.STRING, Types.STRING, Types.STRING)));
        orderListState = getRuntimeContext().getListState(
                new ListStateDescriptor<Order>("order", Types.POJO(Order.class)));
    }

    @Override
    public void processElement1(Tuple8<Boolean, Integer, String, Float, String, String, String, String> left,
                                CoProcessFunction<
                                        Tuple8<Boolean, Integer, String, Float, String, String, String, String>,
                                        Order,
                                        Tuple10<Boolean, Integer, String, Float, String, String, String, String, Integer, Date>>.Context context,
                                Collector<Tuple10<Boolean, Integer, String, Float, String, String, String, String, Integer, Date>> collector) throws Exception {
        leftState.update(left);
        for (Order order : orderListState.get()) {
            Boolean update = left.f0 && order.update;
            Tuple10<Boolean, Integer, String, Float, String, String, String, String, Integer, Date> tuple
                    = Tuple10.of(update, left.f1, left.f2, left.f3, left.f4, left.f5, left.f6, left.f7, order.orderKey, order.orderDate);
            if (update || (left.f0 ^ order.update)) {
                collector.collect(tuple);
            }
        }
    }

    @Override
    public void processElement2(Order order,
                                CoProcessFunction<
                                        Tuple8<Boolean, Integer, String, Float, String, String, String, String>,
                                        Order,
                                        Tuple10<Boolean, Integer, String, Float, String, String, String, String, Integer, Date>>.Context context,
                                Collector<Tuple10<Boolean, Integer, String, Float, String, String, String, String, Integer, Date>> collector) throws Exception {
        orderListState.add(order);
        Tuple8<Boolean, Integer, String, Float, String, String, String, String> left = leftState.value();
        if (left != null) {
            Boolean update = order.update && left.f0;
            Tuple10<Boolean, Integer, String, Float, String, String, String, String, Integer, Date> tuple
                    = Tuple10.of(update, left.f1, left.f2, left.f3, left.f4, left.f5, left.f6, left.f7, order.orderKey, order.orderDate);
            if (update || (left.f0 ^ order.update)) {
                collector.collect(tuple);
            }
        }
    }
}
