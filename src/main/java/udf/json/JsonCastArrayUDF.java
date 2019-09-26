package udf.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
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
 * @date ：Created in 2019-08-16 14:45
 * @description：将jsonarray里的每一个json元素里的value拼接起来，返回新数组
 * @modified By：
 * @version: v1.0$
 */
public class JsonCastArrayUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(JsonCastArrayUDF.class);
    private ListObjectInspector jsonlistInspector;
    private ListObjectInspector listInspector;
    private StringObjectInspector stringInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if(args.length==2){
            jsonlistInspector= (ListObjectInspector) args[0];
            listInspector= (ListObjectInspector) args[1];

        }else if (args.length == 3){
            jsonlistInspector= (ListObjectInspector) args[0];
            listInspector= (ListObjectInspector) args[1];
            stringInspector = (StringObjectInspector) args[2];

        }else{
            throw new UDFArgumentException("Usage : Erro Argument Num");
        }
        ObjectInspector returnOi= PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
        return ObjectInspectorFactory.getStandardListObjectInspector(returnOi);

    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
            List<String> jsonlist = (List<String>) jsonlistInspector.getList(args[0].get());
            List<String> keylist = (List<String>) listInspector.getList(args[1].get());
            if (args.length == 2) {
                return evaluate(jsonlist, keylist);
            } else if (args.length == 3) {
                String s = stringInspector.getPrimitiveJavaObject(args[2].get());
                return evaluate(jsonlist, keylist, s);
            }else{
                return null;
            }

        }


    //双参数evaluate
    public List<String> evaluate(List<String> jsonarray, List<String> array) {
        return evaluate(jsonarray,array,"&");
    }
    public List<String> evaluate(List<String> jsonarray, List<String> array,String splitstr) {

        List<String> list = new ArrayList<String>();
        for (String value : jsonarray) {
            StringBuilder s = new StringBuilder();
            List<String> list1=new ArrayList<String>();
            boolean flag=true;
            for (int j = 0; j < array.size(); ++j) {
                String [] arr=array.get(j).split("\\.");
                String a="null";
                //判断是 普通str||jsonobject||jsonarray
                if (arr.length==0||arr.length==1){ //普通str
                    a = jsonclear(value, array.get(j));
                    if(flag){
                       // 判断是不是最后一个元素
                       if (j == array.size() - 1) s.append(a);
                       else s.append(a).append(splitstr);
                    }else{
                        for(int o=0;o<list1.size();o++){
                            if (j == array.size() - 1)
                                list1.set(o,list.get(o)+a+splitstr);
                            else list1.set(o,list.get(o)+a);
                        }
                    }
                }else if(arr.length==2) { //JSONObject
                    if (jsonJudge(jsonclear(value, arr[0])).equals("JSONObject")) {
                        a = jsonclear(jsonclear(value, arr[0]), arr[1]);
                        // 判断是不是最后一个元素
                        if(flag) {
                            if (j == array.size() - 1) s.append(a);
                            else s.append(a).append(splitstr);
                        }else{
                            for(int o=0;o<list1.size();o++){
                                if (j == array.size() - 1)
                                list1.set(o,list.get(o)+a+splitstr);
                                else list1.set(o,list.get(o)+a);
                            }
                        }

                    }
                }else if(arr.length>2){ //JSONArray
                     if(jsonJudge(jsonclear(value,arr[0])).equals("JSONArray")){
                        flag=false;
                        List<String> list2=new ArrayList<String>();
                        list2=jsonarray(jsonclear(value,arr[0]));
                        for (int k=0;k<list2.size();++k) {
                            StringBuilder ss = s;
                            for (int i = 1; i < arr.length; i++) {
                                 String as=jsonclear(list2.get(k),arr[i]);
                                if (i == arr.length - 1&&j==array.size()-1) ss.append(as);
                                else ss.append(as).append(splitstr);
                            }
                            list1.add(ss.toString());
                        }
                    }
                }


            }
            if (flag) list.add(s.toString());
            else list.addAll(list1);

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

     //判断json字符串是哪种Object
    public String jsonJudge (String jsonstr){
        Object jsonObject=JSON.parse(jsonstr);
        if (jsonObject instanceof JSONObject) {
            return "JSONObject";
        } else if (jsonObject instanceof JSONArray) {
            return "JSONArray";
        }else {
            return null;
        }
    }

    public List<String> jsonarray(String jsonstr){
         List<String> list=JSONObject.parseArray(jsonstr,String.class);
          return  list;
    }





    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }

    public static void main(String[] args) {
        JsonCastArrayUDF jsonCastArrayUDF=new JsonCastArrayUDF();
        List<String> list=new ArrayList<String>();
        list.add("{\"field_id\":22,\"field_name\":\"品类名称\",\"reason\":[{\"id\":1,\"content\":\"品类错误，应为半自助游品类\"}]}");
        List<String> list1=new ArrayList<String>();
        list1.add("field_id");
        list1.add("field_name");
        list1.add("reason.id.content");
        List<String> list3=new ArrayList<String>();
        list3=jsonCastArrayUDF.evaluate(list,list1);
        for(String s : list3){
            System.out.println(s);
        }
    }
}
