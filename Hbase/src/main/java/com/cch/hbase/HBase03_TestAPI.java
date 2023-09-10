package com.cch.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBase03_TestAPI {
    private static Connection connection = Hbase02_SingletonConnection.getConnection();

    public static void main(String[] args) throws IOException {
//        testCreateTable(null, "emp", null);
//        testPutData(null, "stu", "1005", "f1", "name", "niuniu");
//        testDeleteData(null, "stu", "1004", "f1", "name");
//        Hbase02_SingletonConnection.closeConnection();
//        testGetData(null, "stu", "1001");
        testScanData(null, "stu", "0", "2");
    }

    /**
     * DML
     *
     * put: 新增、修改
     *
     * shell: put 'namespaceName:tableName', 'rk', 'cf:cl', 'value'
     */
    public static void testPutData(String namespaceName, String tableName, String rk, String cf, String cl, String value) throws IOException {
        TableName tn = TableName.valueOf(namespaceName, tableName);
        //获取table对象
        Table table = connection.getTable(tn);

        //写入、修改
        Put put = new Put(Bytes.toBytes(rk));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cl), Bytes.toBytes(value));
        table.put(put);

        table.close();
    }

    /**
     * DML
     *
     * delete: 删除
     *
     * shell: delete 'namaspaceName:tableName', 'rk', 'cf:cl', ts   => Delete
     *        deleteall 'namespaceName:tableName', 'rk', 'cf:cl'    => DeleteColumn
     *        deleteall 'namespaceName:tableName', 'rk'             => DeleteFamily
     */
    public static void testDeleteData(String namespaceName, String tableName, String rk, String cf, String cl) throws IOException {
        TableName tn = TableName.valueOf(namespaceName, tableName);
        //获取table对象
        Table table = connection.getTable(tn);

        //删除数据
        Delete delete = new Delete(Bytes.toBytes(rk));
        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cl));
        table.delete(delete);

        table.close();
    }

    /**
     * DML
     *
     * get、scan: 查询
     *
     * shell:   get 'namespaceName:tableName', 'rk'
     *          scan 'namespaceName:tableName', {STARTROW=>'', STOPROW=>''}
     */
    public static void testScanData(String namespaceName, String tableName, String startRow, String stopRow) throws IOException {
        TableName tn = TableName.valueOf(namespaceName, tableName);
        //获取table对象
        Table table = connection.getTable(tn);

        //扫描数据
        Scan scan = new Scan();
        //设置扫描范围
        scan.withStartRow(Bytes.toBytes(startRow)).withStopRow(Bytes.toBytes(stopRow));
        ResultScanner scanner = table.getScanner(scan);
        //迭代处理每行数据
        for (Result result : scanner) {
            //获取每行数据的cell
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String kv = Bytes.toString(CellUtil.cloneRow(cell)) + " : " +
                        Bytes.toString(CellUtil.cloneFamily(cell)) + " : " +
                        Bytes.toString(CellUtil.cloneQualifier(cell)) + " : " +
                        Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(kv);
            }
            System.out.println("----------------------------------------------");
        }
        table.close();
    }
    public static void testGetData(String namespaceName, String tableName, String rk) throws IOException {
        TableName tn = TableName.valueOf(namespaceName, tableName);
        //获取table对象
        Table table = connection.getTable(tn);
        Get get = new Get(Bytes.toBytes(rk));
        Result result = table.get(get);

        //获取到该行数据所有的Cell对象
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            String kv = Bytes.toString(CellUtil.cloneRow(cell)) + " : " +
                    Bytes.toString(CellUtil.cloneFamily(cell)) + " : " +
                    Bytes.toString(CellUtil.cloneQualifier(cell)) + " : " +
                    Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println(kv);
        }

        table.close();
    }



    /**
     * DDL
     *
     * 创建表
     * shell:create 'namespaceName:tableName', 'cf...'
     *
     * 查看表  listTableNames
     * 修改表  modifyTable
     * 删除表  deleteTable
     * 创建库  createNamespace
     * 修改库  modifyNamespace
     * 删除库  deleteNamespace
     * 查看库  listNamespaces
     */
    public static void testCreateTable(String namespaceName, String tableName, String ... cfs) throws IOException {
        //如果是功能代码，未来可能会被调用，方法在写的时候，就需要考虑到健壮性。 因为，基本的参数判空操作就一定要有。

        //对于namespaceName， 可以传入可以不传，如果不传，默认使用default

        if (namespaceName ==null || namespaceName.trim().isEmpty()){
            namespaceName = "default";
        }
        if (tableName ==null || tableName.trim().isEmpty()){
            throw new RuntimeException("表名不能为空！！！");
        }
        if (cfs == null || cfs.length <= 0){
            throw new RuntimeException("至少指定一个列族！！！");
        }

        Admin admin = connection.getAdmin();

        //判断表是否已经存在
        TableName tn = TableName.valueOf(namespaceName, tableName);
        boolean tableExists = admin.tableExists(tn);
        if (tableExists){
            throw new RuntimeException(namespaceName == null ? "default" : namespaceName + ":" + tableName + " 表已经存在");
        }

        //创建表
        List<ColumnFamilyDescriptor> cfdb = new ArrayList<>();
        for ( String  cf : cfs){
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
            cfdb.add(columnFamilyDescriptorBuilder.build());
        }
        TableDescriptor tableDescriptor = TableDescriptorBuilder
                .newBuilder(tn)
                //设置列族信息
                .setColumnFamilies(cfdb)
                .build();
        admin.createTable(tableDescriptor);

        //建表成功
        System.out.println(namespaceName == null ? "default" : namespaceName + ":" + tableName + " 表创建成功");

        admin.close();
    }


    //get
    public static String testGetcell(String namespaceName , String tableName , String rowKey , String family,String column , String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespaceName, tableName));

        Get get = new Get(Bytes.toBytes(rowKey));

        get.addColumn(Bytes.toBytes(family),Bytes.toBytes(value));

        Result result = table.get(get);

        String values = " ";
        for (Cell cell : result.rawCells()) {
            values += Bytes.toString(CellUtil.cloneValue(cell)) + "-";
        }

        table.close();
        return values;
    }

//del

    public static void testDel(String namespaceName , String tableName , String rowKey , String family,String column , String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespaceName, rowKey));

        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //删除单个版本
        delete.addColumn(Bytes.toBytes(family),Bytes.toBytes(column));

        //删除某个列组
        delete.addFamily(Bytes.toBytes(family));
        table.delete(delete);

        table.close();

    }

    public static List<String> testScan(String namespaceName , String tableName , String startRow, String stopRow) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespaceName, tableName));

        Scan scan = new Scan().withStartRow(Bytes.toBytes(startRow)).withStopRow(Bytes.toBytes(stopRow));

        //扫描
        ResultScanner scanner = table.getScanner(scan);

        //获取结果
        ArrayList<String> arrayList = new ArrayList<>();
        for (Result result : scanner) {
            arrayList.add(Bytes.toString(result.value()));
        }

        scanner.close();
        table.close();
        return arrayList;
    }

    public static void testFilter(String namespaceName , String tableName , String startRow, String stopRow,String colFamily,String colunm,String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespaceName, tableName));

        Scan scan = new Scan();

        scan.withStartRow(Bytes.toBytes(startRow)).withStopRow(Bytes.toBytes(stopRow));

        //过滤器

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        //列值过滤器
        ColumnValueFilter columnValueFilter = new ColumnValueFilter(
                //列族
                Bytes.toBytes(colFamily),
                //列名
                Bytes.toBytes(colunm),
                //规则
                CompareOperator.EQUAL,
                Bytes.toBytes(value)
        );

        //单列值过滤器
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                //列族
                Bytes.toBytes(colFamily),
                Bytes.toBytes(colunm),
                CompareOperator.EQUAL,
                Bytes.toBytes(value)
        );

        scan.setFilter(singleColumnValueFilter);

        for (Result result : table.getScanner(scan)) {
            for (Cell cell : result.rawCells()) {
                System.out.print(new String(CellUtil.cloneRow(cell))+
                        "-" + new String(CellUtil.cloneFamily(cell))
                        + "-" + new String(CellUtil.cloneQualifier(cell))
                        + "-" + new String(CellUtil.cloneValue(cell)) + '\t');
            }
            System.out.println();
        }
        table.close();
    }


}
