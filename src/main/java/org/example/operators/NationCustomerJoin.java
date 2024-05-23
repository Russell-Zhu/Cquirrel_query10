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
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;
import org.example.Relations.Customer;
import org.example.Relations.Nation;
import org.example.Relations.Order;

import java.sql.Date;
// left: Nation
// right: Customer
// return update, customer.custKey, customer.name, customer.acctbal, nation.name, customer.address, customer.phone, customer.comment
// Tuple8<Boolean, Integer, String, Float, String, String, String, String>


public class NationCustomerJoin extends CoProcessFunction<
        Nation,
        Customer,
        Tuple8<Boolean, Integer, String, Float, String, String, String, String>> {

    private ValueState<Nation> nationValueState;
    private ListState<Customer> customerListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        nationValueState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("nation", Types.POJO(Nation.class)));
        customerListState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("customer", Types.POJO(Customer.class)));
    }

    @Override
    public void processElement1(Nation nation,
                                CoProcessFunction<
                                        Nation,
                                        Customer,
                                        Tuple8<Boolean, Integer, String, Float, String, String, String, String>>.Context context,
                                Collector<Tuple8<Boolean, Integer, String, Float, String, String, String, String>> collector) throws Exception {
        nationValueState.update(nation);
        for (Customer customer : customerListState.get()) {
            Boolean update = nation.update && customer.update;
            Tuple8<Boolean, Integer, String, Float, String, String, String, String> tuple = Tuple8.of(update, customer.custKey, customer.name, customer.acctbal, nation.name, customer.address, customer.phone, customer.comment);
            if (update || (nation.update ^ customer.update)) {
                collector.collect(tuple);
            }
        }
    }

    @Override
    public void processElement2(Customer customer,
                                CoProcessFunction<
                                        Nation,
                                        Customer,
                                        Tuple8<Boolean, Integer, String, Float, String, String, String, String>>.Context context,
                                Collector<Tuple8<Boolean, Integer, String, Float, String, String, String, String>> collector) throws Exception {
        customerListState.add(customer);

        Nation nation = nationValueState.value();

        if (nation != null) {
            Boolean update = customer.update && nation.update;
            Tuple8<Boolean, Integer, String, Float, String, String, String, String> tuple = Tuple8.of(update, customer.custKey, customer.name, customer.acctbal, nation.name, customer.address, customer.phone, customer.comment);
            if (update || (customer.update ^ nation.update)) {
                collector.collect(tuple);
            }
        }
    }
}
