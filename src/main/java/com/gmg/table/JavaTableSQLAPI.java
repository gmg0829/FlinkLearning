package com.gmg.table;

/**
 * @author gmg
 * @title: JavaTableSQLAPI
 * @projectName IntelligentOperationFlink
 * @description: TODO
 * @date 2020/2/29 13:48
 */
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Author: Michael PK
 */
public class JavaTableSQLAPI {

    public static void main(String[] args) throws Exception {


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);

        String filePath = JavaTableSQLAPI.class.getResource("/sales.csv").getPath();

        DataSet<Sales> csv = env.readCsvFile(filePath)
                .ignoreFirstLine()
                .pojoType(Sales.class,"transactionId","customerId","itemId","amountPaid");
        //csv.print();

        Table sales = tableEnv.fromDataSet(csv);
        tableEnv.registerTable("sales", sales);
        Table resultTable = tableEnv.sqlQuery("select customerId, sum(amountPaid) money from sales group by customerId");

        DataSet<Row> result = tableEnv.toDataSet(resultTable, Row.class);
        result.print();
    }

    public static class Sales{
        public String transactionId;
        public String customerId;
        public String itemId;
        public Double  amountPaid;

    }
}

