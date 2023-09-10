package com.cch.hive.UDF;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
        TODO hive自定义函数
        * 1. 继承Hive提供的 GenericUDF类
        * 需求:制定一个计算字符串的长度 （一进一出）
        * */
public class DIV_UDF extends GenericUDF {

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //1.校验参数个数 判断arguments不能为空
        if (arguments.length != 1){
            throw new UDFArgumentLengthException("当前参数长度不正确！！！");
        }
        //2.校验参数的数据类型
        ObjectInspector argument = arguments[0];
        if (!argument.getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentTypeException(0,"当前参数数据类型不正确，请输入基本数据类型...");
        }
        //3.校验当前参数是否为String类型
        PrimitiveObjectInspector primitiveCategory = (PrimitiveObjectInspector) argument;
        if (!primitiveCategory.getPrimitiveCategory()
                .equals(PrimitiveObjectInspector.PrimitiveCategory.STRING)){
            throw new UDFArgumentTypeException(0,"当前参数数据类型不正确，请输入String类型...");
        }

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        DeferredObject argument = arguments[0];
        Object arg = argument.get();
        if (arg == null){
            return 0;
        }
        return arg.toString().length();
    }

    public String getDisplayString(String[] strings) {
        return null;
    }
}
