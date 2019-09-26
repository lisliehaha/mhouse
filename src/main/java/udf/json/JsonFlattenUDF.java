package udf.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ：liusengen
 * @date ：Created in 2019-08-19 09:49
 * @description：
 * @modified By：
 * @version: $
 */
public class JsonFlattenUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(JsonFlattenUDF.class);
    private ListObjectInspector jsonlistInspector;
    private StringObjectInspector stringInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if(args.length==1){
            jsonlistInspector= (ListObjectInspector) args[0];

        }else if (args.length == 2){
            jsonlistInspector= (ListObjectInspector) args[0];
            stringInspector = (StringObjectInspector) args[1];

        }else{
            throw new UDFArgumentException("Usage : Erro Argument Num");
        }
        ObjectInspector returnOi= PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
        return ObjectInspectorFactory.getStandardListObjectInspector(returnOi);

    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        try {
            List<?> jsonlist = jsonlistInspector.getList(args[0].get());
            if (args.length == 1) {
                return evaluate((List<String>) jsonlist);
            } else if (args.length == 2) {
                String s = stringInspector.getPrimitiveJavaObject(args[1].get());
                return evaluate((List<String>) jsonlist, s);
            } else {
                return null;

            }
        }catch (Exception e){
            return null;
        }

    }


    //双参数evaluate
    public List<String> evaluate(List<String> jsonarray) {
        return evaluate(jsonarray,"&");
    }
    public List<String> evaluate(List<String> jsonarray,String splitstr) {

        List<String> list = new ArrayList<String>();
        for (String value : jsonarray) {
            String field_id=jsonclear(value,"field_id");
            String field_name = jsonclear(value,"field_name");
            String reason = jsonclear(value,"reason");
            List<String> resonlist = jsonarray(reason);
            List<String> resonlist2 = new ArrayList<String>();
            for(int i=0;i<resonlist.size();i++){
                String id=jsonclear(resonlist.get(i),"id");
                String content = jsonclear(resonlist.get(i),"content");
                resonlist2.add(id+splitstr+content);
            }
            for(String va:resonlist2){
                String re=field_id+splitstr+field_name+splitstr+va;
                list.add(re);
            }
        }
        return list;
    }

    //解析json
    public String jsonclear(String jsonstr,String key){
        JSONObject jsonObject=new JSONObject();
        jsonObject= JSON.parseObject(jsonstr);
        String retval = jsonObject.getString(key);
        return retval;
    }


    public List<String> jsonarray(String jsonstr){
        List<String> list=JSONObject.parseArray(jsonstr,String.class);
        return  list;
    }





    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }


}
