package udf.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ：liusengen
 * @date ：Created in 2019/9/20 4:36 下午
 * @description：
 * @modified By：
 * @version: $
 */
public class JsonArrayElementArray extends GenericUDF {
    private StringObjectInspector stringInspector;
    private StringObjectInspector stringInspector2;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length == 2){
            stringInspector= (StringObjectInspector) args[0];
            stringInspector2 = (StringObjectInspector) args[1];
        }else{
            throw new UDFArgumentException("Usage : Erro Argument Num");
        }
        ObjectInspector returnOi= PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
        return ObjectInspectorFactory.getStandardListObjectInspector(returnOi);
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        String jsonarray = stringInspector.getPrimitiveJavaObject(args[0].get());
        String target = stringInspector2.getPrimitiveJavaObject(args[1].get());

        List<String> re= new ArrayList<String>();
        try {
            List<String> list = JSONObject.parseArray(jsonarray, String.class);
            for (String s : list) {
                JSONObject jsonObject = new JSONObject();
                jsonObject = JSON.parseObject(s);
                if(!jsonObject.getString(target).equals("") && jsonObject.getString(target)!=null) {
                    re.add(jsonObject.getString(target));
                }
            }
        }catch (Exception e){
            return null;
        }
        return re;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
