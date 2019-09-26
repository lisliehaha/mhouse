package udf.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.List;

/**
 * @author ：liusengen
 * @date ：Created in 2019/9/20 10:07 上午
 * @description：
 * @modified By：
 * @version: $
 */
public class JsonArrayElementJoint  extends UDF {
    public Text evaluate(Text jsonarray,Text target,Text split){
        if(jsonarray ==null){
            return null;
        }
        String jsonarray2=jsonarray.toString();
        String tar = target.toString();
        String sp = split.toString();
        StringBuilder re= new StringBuilder();
        try {
          List<String> list = JSONObject.parseArray(jsonarray2, String.class);
            for (String s : list) {
                JSONObject jsonObject = new JSONObject();
                jsonObject = JSON.parseObject(s);
                if(!jsonObject.getString(tar).equals("") && jsonObject.getString(tar)!=null) {
                    re.append(jsonObject.getString(tar));
                    re.append(sp);
                }
            }
        }catch (Exception e){
            return null;
        }
        try {
            re = new StringBuilder(re.substring(0, re.length() - sp.length()));
        }catch (Exception e){
            return null;
        }

        return new Text(re.toString());

    }


    public static void main(String[] args) {
        String jsonarray="[{\"name\": \"逢简水乡\"{\"name\": \"逢简水乡\"}]";
        Text text=new Text(jsonarray);
        JsonArrayElementJoint jsonArrayElementJoint=new JsonArrayElementJoint();
        System.out.println(jsonArrayElementJoint.evaluate(text,new Text("name"),new Text("->")));
        String s="da&";
        System.out.println(s.substring(0,2));
    }
}
