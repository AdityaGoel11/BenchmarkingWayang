/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.sql;

import org.apache.wayang.basic.data.Record;

import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.spark.Spark;

import java.io.Serializable;
import java.util.*;


public class SqlTest {


    public static void main(String[] args) {
        long starttime = System.nanoTime();
        WayangPlan wayangPlan;
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/tpch2");
        configuration.setProperty("wayang.postgres.jdbc.user", "aditya");
        configuration.setProperty("wayang.postgres.jdbc.password", "12345678");

        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
//                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin());

        Collection<Record> collector = new ArrayList<>();

        TableSource lineitemSource = new PostgresTableSource("lineitem");

        MapOperator<Record, LineItemTuple> parser = new MapOperator<>(
                (record) -> new LineItemTuple.Parser().parse(record), Record.class, LineItemTuple.class
        );
        lineitemSource.connectTo(0, parser, 0);

        MapOperator<LineItemTuple, ReturnTuple> projection = new MapOperator<>(
                (lineItemTuple) -> new ReturnTuple(
                        lineItemTuple.L_RETURNFLAG,
                        lineItemTuple.L_LINESTATUS,
                        lineItemTuple.L_QUANTITY,
                        lineItemTuple.L_EXTENDEDPRICE,
                        lineItemTuple.L_EXTENDEDPRICE * (1 - lineItemTuple.L_DISCOUNT),
                        lineItemTuple.L_EXTENDEDPRICE * (1 - lineItemTuple.L_DISCOUNT) * (1 + lineItemTuple.L_TAX),
                        lineItemTuple.L_QUANTITY,
                        lineItemTuple.L_EXTENDEDPRICE,
                        lineItemTuple.L_DISCOUNT,
                        1),
                LineItemTuple.class,
                ReturnTuple.class
        );
        parser.connectTo(0, projection, 0);

        // Aggregation phase 1.
        ReduceByOperator<ReturnTuple, GroupKey> aggregation = new ReduceByOperator<>(
                (returnTuple) -> new GroupKey(returnTuple.L_RETURNFLAG, returnTuple.L_LINESTATUS),
                ((t1, t2) -> {
                    t1.SUM_QTY += t2.SUM_QTY;
                    t1.SUM_BASE_PRICE += t2.SUM_BASE_PRICE;
                    t1.SUM_DISC_PRICE += t2.SUM_DISC_PRICE;
                    t1.SUM_CHARGE += t2.SUM_CHARGE;
                    t1.AVG_QTY += t2.AVG_QTY;
                    t1.AVG_PRICE += t2.AVG_PRICE;
                    t1.AVG_DISC += t2.AVG_DISC;
                    t1.COUNT_ORDER += t2.COUNT_ORDER;
                    return t1;
                }),
                GroupKey.class,
                ReturnTuple.class
        );
        projection.connectTo(0, aggregation, 0);

        // Aggregation phase 2: complete AVG operations.
        MapOperator<ReturnTuple, ReturnTuple> aggregationFinalization = new MapOperator<>(
                (t -> {
                    t.AVG_QTY /= t.COUNT_ORDER;
                    t.AVG_PRICE /= t.COUNT_ORDER;
                    t.AVG_DISC /= t.COUNT_ORDER;
                    return t;
                }),
                ReturnTuple.class,
                ReturnTuple.class
        );
        aggregation.connectTo(0, aggregationFinalization, 0);

        // TODO: Implement sorting (as of now not possible with Wayang's basic operators).

        // Print the results.
        LocalCallbackSink<LineItemTuple> sink = LocalCallbackSink.createStdoutSink(LineItemTuple.class);
        aggregation.connectTo(0, sink, 0);
//        aggregationFinalization.connectTo(0, sink, 0);


        wayangPlan = new WayangPlan(sink);

        wayangContext.execute("PostgreSql test", wayangPlan);
        long endTime = System.nanoTime();
        long duration = (endTime - starttime)/1000000;
        System.out.println("Execution time: " + duration + " milliseconds");

    }
}


class ReturnTuple implements Serializable {

    public char L_RETURNFLAG;

    public char L_LINESTATUS;

    public double SUM_QTY;

    public double SUM_BASE_PRICE;

    public double SUM_DISC_PRICE;

    public double SUM_CHARGE;

    public double AVG_QTY;

    public double AVG_PRICE;

    public double AVG_DISC;

    public int COUNT_ORDER;

    public ReturnTuple() {
    }

    public ReturnTuple(char l_RETURNFLAG,
                       char l_LINESTATUS,
                       double SUM_QTY,
                       double SUM_BASE_PRICE,
                       double SUM_DISC_PRICE,
                       double SUM_CHARGE,
                       double AVG_QTY,
                       double AVG_PRICE,
                       double AVG_DISC,
                       int COUNT_ORDER) {
        this.L_RETURNFLAG = l_RETURNFLAG;
        this.L_LINESTATUS = l_LINESTATUS;
        this.SUM_QTY = SUM_QTY;
        this.SUM_BASE_PRICE = SUM_BASE_PRICE;
        this.SUM_DISC_PRICE = SUM_DISC_PRICE;
        this.SUM_CHARGE = SUM_CHARGE;
        this.AVG_QTY = AVG_QTY;
        this.AVG_PRICE = AVG_PRICE;
        this.AVG_DISC = AVG_DISC;
        this.COUNT_ORDER = COUNT_ORDER;
    }

    @Override
    public String toString() {
        return "ReturnTuple{" +
                "L_RETURNFLAG=" + this.L_RETURNFLAG +
                ", L_LINESTATUS=" + this.L_LINESTATUS +
                ", SUM_QTY=" + this.SUM_QTY +
                ", SUM_BASE_PRICE=" + this.SUM_BASE_PRICE +
                ", SUM_DISC_PRICE=" + this.SUM_DISC_PRICE +
                ", SUM_CHARGE=" + this.SUM_CHARGE +
                ", AVG_QTY=" + this.AVG_QTY +
                ", AVG_PRICE=" + this.AVG_PRICE +
                ", AVG_DISC=" + this.AVG_DISC +
                ", COUNT_ORDER=" + this.COUNT_ORDER +
                '}';
    }
}


class LineItemTuple implements Serializable {

    /**
     * {@code identifier}, {@code PK}
     */
    public long L_ORDERKEY;

    /**
     * {@code identifier}
     */
    public long L_PARTKEY;

    /**
     * {@code identifier}
     */
    public long L_SUPPKEY;

    /**
     * {@code integer}, {@code PK}
     */
    public int L_LINENUMBER;

    /**
     * {@code decimal}
     */
    public double L_QUANTITY;

    /**
     * {@code decimal}
     */
    public double L_EXTENDEDPRICE;

    /**
     * {@code decimal}
     */
    public double L_DISCOUNT;

    /**
     * {@code decimal}
     */
    public double L_TAX;

    /**
     * {@code fixed text, size 1}
     */
    public char L_RETURNFLAG;

    /**
     * {@code fixed text, size 1}
     */
    public char L_LINESTATUS;

    /**
     * {@code fixed text, size 1}
     */
    public int L_SHIPDATE;

    /**
     * {@code fixed text, size 1}
     */
    public int L_COMMITDATE;

    /**
     * {@code fixed text, size 1}
     */
    public int L_RECEIPTDATE;

    /**
     * {@code fixed text, size 25}
     */
    public String L_SHIPINSTRUCT;

    /**
     * {@code fixed text, size 10}
     */
    public String L_SHIPMODE;

    /**
     * {@code variable text, size 44}
     */
    public String L_COMMENT;

    public LineItemTuple(long l_ORDERKEY,
                         long l_PARTKEY,
                         long l_SUPPKEY,
                         int l_LINENUMBER,
                         double l_QUANTITY,
                         double l_EXTENDEDPRICE,
                         double l_DISCOUNT,
                         double l_TAX,
                         char l_RETURNFLAG,
                         int l_SHIPDATE,
                         int l_COMMITDATE,
                         int l_RECEIPTDATE,
                         String l_SHIPINSTRUCT,
                         String l_SHIPMODE,
                         String l_COMMENT) {
        this.L_ORDERKEY = l_ORDERKEY;
        this.L_PARTKEY = l_PARTKEY;
        this.L_SUPPKEY = l_SUPPKEY;
        this.L_LINENUMBER = l_LINENUMBER;
        this.L_QUANTITY = l_QUANTITY;
        this.L_EXTENDEDPRICE = l_EXTENDEDPRICE;
        this.L_DISCOUNT = l_DISCOUNT;
        this.L_TAX = l_TAX;
        this.L_RETURNFLAG = l_RETURNFLAG;
        this.L_SHIPDATE = l_SHIPDATE;
        this.L_COMMITDATE = l_COMMITDATE;
        this.L_RECEIPTDATE = l_RECEIPTDATE;
        this.L_SHIPINSTRUCT = l_SHIPINSTRUCT;
        this.L_SHIPMODE = l_SHIPMODE;
        this.L_COMMENT = l_COMMENT;
    }

    public LineItemTuple() {
    }

    public static class Parser {

        public LineItemTuple parse(Record record) {
            LineItemTuple tuple = new LineItemTuple();
//            System.out.println(record);

            int l = record.size();
            int i = 0;

            tuple.L_ORDERKEY = record.getLong(i);
            i++;
            tuple.L_PARTKEY = record.getLong(i);
            i++;
            tuple.L_SUPPKEY = record.getLong(i);
            i++;
            tuple.L_LINENUMBER = record.getInt(i);
            i++;
            tuple.L_QUANTITY = record.getDouble(i);
            i++;
            tuple.L_EXTENDEDPRICE = record.getDouble(i);
            i++;
            tuple.L_DISCOUNT = record.getDouble(i);
            i++;
            tuple.L_TAX = record.getDouble(i);
            i++;
            tuple.L_RETURNFLAG = record.getString(i).charAt(0);
            i++;
            tuple.L_LINESTATUS = record.getString(i).charAt(0);
            i++;
            tuple.L_SHIPDATE = parseDate(record.getString(i));
            i++;
            tuple.L_COMMITDATE = parseDate(record.getString(i));
            i++;
            tuple.L_RECEIPTDATE = parseDate(record.getString(i));
            i++;
            tuple.L_SHIPINSTRUCT = record.getString(i);
            i++;
            tuple.L_SHIPMODE = record.getString(i);
            i++;
            tuple.L_COMMENT = record.getString(i);

//            int startPos = 0;
//            int endPos = line.indexOf(';', startPos);
//            tuple.L_ORDERKEY = Long.valueOf(line.substring(startPos + 1, endPos - 1));
//
//            startPos = endPos + 1;
//            endPos = line.indexOf(';', startPos);
//            tuple.L_PARTKEY = Long.valueOf(line.substring(startPos + 1, endPos - 1));
//
//            startPos = endPos + 1;
//            endPos = line.indexOf(';', startPos);
//            tuple.L_SUPPKEY = Long.valueOf(line.substring(startPos + 1, endPos - 1));
//
//            startPos = endPos + 1;
//            endPos = line.indexOf(';', startPos);
//            tuple.L_LINENUMBER = Integer.valueOf(line.substring(startPos + 1, endPos - 1));
//
//            startPos = endPos + 1;
//            endPos = line.indexOf(';', startPos);
//            tuple.L_QUANTITY = Double.valueOf(line.substring(startPos + 1, endPos - 1));
//
//            startPos = endPos + 1;
//            endPos = line.indexOf(';', startPos);
//            tuple.L_EXTENDEDPRICE = Double.valueOf(line.substring(startPos + 1, endPos - 1));
//
//            startPos = endPos + 1;
//            endPos = line.indexOf(';', startPos);
//            tuple.L_DISCOUNT = Double.valueOf(line.substring(startPos + 1, endPos - 1));
//
//            startPos = endPos + 1;
//            endPos = line.indexOf(';', startPos);
//            tuple.L_TAX = Double.valueOf(line.substring(startPos + 1, endPos - 1));
//
//            startPos = endPos + 1;
//            endPos = line.indexOf(';', startPos);
//            tuple.L_RETURNFLAG = line.charAt(startPos + 1);
//
//            startPos = endPos + 1;
//            endPos = line.indexOf(';', startPos);
//            tuple.L_LINESTATUS = line.charAt(startPos + 1);
//
//            startPos = endPos + 1;
//            endPos = line.indexOf(';', startPos);
//            tuple.L_SHIPDATE = parseDate(line.substring(startPos + 1, endPos - 1));
//
//            startPos = endPos + 1;
//            endPos = line.indexOf(';', startPos);
//            tuple.L_COMMITDATE = parseDate(line.substring(startPos + 1, endPos - 1));
//
//            startPos = endPos + 1;
//            endPos = line.indexOf(';', startPos);
//            tuple.L_RECEIPTDATE = parseDate(line.substring(startPos + 1, endPos - 1));
//
//            startPos = endPos + 1;
//            endPos = startPos - 1;
//            do {
//                endPos++;
//                endPos = line.indexOf(';', endPos);
//            } while (line.charAt(endPos - 1) != '"' || line.charAt(endPos + 1) != '"');
//            tuple.L_SHIPINSTRUCT = line.substring(startPos + 1, endPos - 1);
//
//            startPos = endPos + 1;
//            endPos = startPos - 1;
//            do {
//                endPos++;
//                endPos = line.indexOf(';', endPos);
//            } while (line.charAt(endPos - 1) != '"' || line.charAt(endPos + 1) != '"');
//            tuple.L_SHIPMODE = line.substring(startPos + 1, endPos - 1);
//
//            startPos = endPos + 1;
//            endPos = startPos - 1;
//            do {
//                endPos++;
//                endPos = line.indexOf(';', endPos);
//            } while (endPos >= 0 && (line.charAt(endPos - 1) != '"' || (endPos < line.length() - 1 && line.charAt(endPos + 1) != '"')));
//            assert endPos < 0 : String.format("Parsing error: unexpected ';' at %d. Input: %s", endPos, line);
//            endPos = line.length();
//            tuple.L_COMMENT = line.substring(startPos + 1, endPos - 1);

            return tuple;
        }

        public static int parseDate(String dateString) {
            Calendar calendar = GregorianCalendar.getInstance();
            calendar.set(
                    Integer.parseInt(dateString.substring(0, 4)),
                    Integer.parseInt(dateString.substring(5, 7)) - 1,
                    Integer.parseInt(dateString.substring(8, 10))
            );
            final int millisPerDay = 1000 * 60 * 60 * 24;
            return (int) (calendar.getTimeInMillis() / millisPerDay);
        }


    }
}

class GroupKey implements Serializable {

    public char L_RETURNFLAG;

    public char L_LINESTATUS;

    public GroupKey(char l_RETURNFLAG, char l_LINESTATUS) {
        this.L_RETURNFLAG = l_RETURNFLAG;
        this.L_LINESTATUS = l_LINESTATUS;
    }

    public GroupKey() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        GroupKey groupKey = (GroupKey) o;
        return this.L_RETURNFLAG == groupKey.L_RETURNFLAG &&
                this.L_LINESTATUS == groupKey.L_LINESTATUS;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.L_RETURNFLAG, this.L_LINESTATUS);
    }
}