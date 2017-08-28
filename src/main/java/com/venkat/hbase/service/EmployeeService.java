package com.venkat.hbase.service;

import com.venkat.hbase.common.Employee;
import com.venkat.hbase.common.HBaseConnection;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by venkatram.veerareddy on 8/28/2017.
 */
public class EmployeeService {
    @Autowired
    private HBaseConnection hBaseConnection;

    public List<Employee> getAllEmployees() {
        final Scan scan = new Scan();
        ResultScanner scanner = null;
        Table table = null;
        List<Employee> empList = new ArrayList<>();
        try  {
            Connection conn = hBaseConnection.getConnection();
            table = conn.getTable(TableName.valueOf("tempTable"));
            scan.addColumn(Bytes.toBytes("myColumnFamily"), Bytes.toBytes("empNo"));
            scan.addColumn(Bytes.toBytes("myColumnFamily"), Bytes.toBytes("firstName"));
            scan.addColumn(Bytes.toBytes("myColumnFamily"), Bytes.toBytes("fastName"));
            scan.addColumn(Bytes.toBytes("myColumnFamily"), Bytes.toBytes("email"));
            for(Result result : table.getScanner(scan)){
                Employee e = new Employee();
                //getting row key
                //Bytes.toString(result.getRow())
                byte[] bytes = result.getValue(Bytes.toBytes("myColumnFamily"),Bytes.toBytes("empId"));
                e.setEmpNo(Bytes.toString(bytes));

                bytes = result.getValue(Bytes.toBytes("myColumnFamily"),Bytes.toBytes("firstName"));
                e.setFirstName(Bytes.toString(bytes));

                bytes = result.getValue(Bytes.toBytes("myColumnFamily"),Bytes.toBytes("lastName"));
                e.setLastName(Bytes.toString(bytes));

                bytes = result.getValue(Bytes.toBytes("myColumnFamily"),Bytes.toBytes("email"));
                e.setEmail(Bytes.toString(bytes));

                empList.add(e);
            }
        } catch (Exception e) {
            throw new RuntimeException("Exception while calling getAllEmployees() ", e);
        } finally {
            try {
                if(table != null) {
                    table.close();
                }
            } catch (IOException e) {
                System.err.println("Exception while closing table ");
            }
        }
        return empList;
    }



    public Employee addEmployee(Employee emp) {

        try {
            Connection conn = hBaseConnection.getConnection();
            Table table = conn.getTable(TableName.valueOf("tempTable"));

            //Put put = new Put("3".getBytes());
            Put put = new Put(emp.getEmpNo().getBytes());
            put.addColumn("myColumnFamily".getBytes(),"empNo".getBytes(),emp.getEmpNo().getBytes());
            put.addColumn("myColumnFamily".getBytes(),"firstName".getBytes(),emp.getFirstName().getBytes());
            put.addColumn("myColumnFamily".getBytes(),"fastName".getBytes(),emp.getLastName().getBytes());
            put.addColumn("myColumnFamily".getBytes(),"email".getBytes(),emp.getEmail().getBytes());
            table.put(put);
            table.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return emp;
    }

    public Employee getEmployee(String empId) {

        Employee emp = new Employee();
        try {
            Connection conn = hBaseConnection.getConnection();
            Table table = conn.getTable(TableName.valueOf("tempTable"));

            Get get = new Get(empId.getBytes());

            Result result = table.get(get);

            byte[] bytes = result.getValue(Bytes.toBytes("myColumnFamily"),Bytes.toBytes("empNo"));
            emp.setEmpNo(Bytes.toString(bytes));

            bytes = result.getValue(Bytes.toBytes("myColumnFamily"),Bytes.toBytes("firstName"));
            emp.setFirstName(Bytes.toString(bytes));

            bytes = result.getValue(Bytes.toBytes("myColumnFamily"),Bytes.toBytes("lastName"));
            emp.setLastName(Bytes.toString(bytes));

            bytes = result.getValue(Bytes.toBytes("myColumnFamily"),Bytes.toBytes("email"));
            emp.setEmail(Bytes.toString(bytes));


            table.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return emp;
    }

    public boolean deleteEmployee(String empId) {
        boolean deleted = false;
        try {
            Connection conn = hBaseConnection.getConnection();
            Table table = conn.getTable(TableName.valueOf("tempTable"));
            Get get = new Get(empId.getBytes());
            Result result = table.get(get);
            if(result != null) {
                Delete delete = new Delete(result.getRow());
                table.delete(delete);
                deleted = true;
            }
            table.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return deleted;
    }


    /*public List<Employee> getAllEmployees(){
        List<Employee> empList = new ArrayList<>();

        HTable table;
        try {
            table = new HTable(hBaseConnection.getConnection(), "tempTable");
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("myColumnFamily"), Bytes.toBytes("empNo"));
            scan.addColumn(Bytes.toBytes("myColumnFamily"), Bytes.toBytes("firstName"));
            scan.addColumn(Bytes.toBytes("myColumnFamily"), Bytes.toBytes("fastName"));
            scan.addColumn(Bytes.toBytes("myColumnFamily"), Bytes.toBytes("email"));

            ResultScanner scanner = table.getScanner(scan);
            for(Result result : scanner){
                Employee e = new Employee();

                byte[] bytes = result.getValue(Bytes.toBytes("myColumnFamily"),Bytes.toBytes("empNo"));
                e.setEmpNo(Bytes.toString(bytes));

                bytes = result.getValue(Bytes.toBytes("myColumnFamily"),Bytes.toBytes("firstName"));
                e.setFirstName(Bytes.toString(bytes));

                bytes = result.getValue(Bytes.toBytes("myColumnFamily"),Bytes.toBytes("lastName"));
                e.setLastName(Bytes.toString(bytes));

                bytes = result.getValue(Bytes.toBytes("myColumnFamily"),Bytes.toBytes("email"));
                e.setEmail(Bytes.toString(bytes));

                empList.add(e);
            }

            scanner.close();
            table.close();

        }catch(IOException ex){
            ex.printStackTrace();
        }
        return empList;
    }*/
}
