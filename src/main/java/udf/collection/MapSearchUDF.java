package udf.collection;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.log4j.Logger;

import java.util.Map;
/**
 * @author ：liusengen
 * @date ：Created in 2019-08-16 10:42
 * @description： 输入json的array字符串，返回array类型
 * @modified By：
 * @version: v1.0$
 */

/*
*   add jar /tmp/pre-1.0-SNAPSHOT.jar
*   create temporary function mapmax as 'udf.collection.MapSearchUDF'
*   select mapmax(str_to_map('aaa:0&bbb:0', '&', ':'),'max')
* */
public class MapSearchUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(MapSearchUDF.class);
    private StringObjectInspector strInspector;
    private MapObjectInspector mapInspector;
    private PrimitiveObjectInspector mapKeyInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if(args.length==2){
            mapInspector= (MapObjectInspector) args[0];
            strInspector= (StringObjectInspector) args[1];
            mapKeyInspector = (PrimitiveObjectInspector) mapInspector.getMapKeyObjectInspector();
        }else{
            throw new UDFArgumentException("erro Argument number");
        }
        ObjectInspector keyInsp = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        return keyInsp ;
    }

    @Override
    public String evaluate(DeferredObject[] args) throws HiveException {
       try {
           Map<?, ?> map = mapInspector.getMap(args[0].get());
           String arg1 = strInspector.getPrimitiveJavaObject(args[1].get());
           int maxvalue = Integer.MIN_VALUE;
           int minvalue = Integer.MAX_VALUE;
           String maxkey = null;
           String minkey = null;
           for (Map.Entry<?, ?> e : map.entrySet()) {
               int value = Integer.parseInt(String.valueOf(mapKeyInspector.getPrimitiveJavaObject(e.getValue())));
               if (value > maxvalue) {
                   maxvalue = value;
                   maxkey = (String) mapKeyInspector.getPrimitiveJavaObject(e.getKey());
               }
               if (value < minvalue) {
                   minvalue = value;
                   minkey = (String) mapKeyInspector.getPrimitiveJavaObject(e.getKey());
               }
           }

           if (arg1.equals("max")) {
               if(maxvalue==0){
                   return null;
               }
                   return maxkey;
           } else if (arg1.equals("min")) {
                   return minkey;
           } else {
               return null;
           }
       }catch (Exception e){
           return null;
       }

    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
