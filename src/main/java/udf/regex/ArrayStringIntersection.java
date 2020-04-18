package udf.regex;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ArrayStringIntersection extends UDF {
    public Text evaluate (Text s,Text p,Text f){
        if(s == null || p == null) return null;
        String[] ss = s.toString().split(f.toString());
        String [] pp = p.toString().split(f.toString());
        StringBuilder res = new StringBuilder();
        for (String value : ss) { //考虑 ss 和 pp 长度很短，直接二层循环
            for (int j = 0; j < pp.length; j++) {
                if (value.equals(pp[j])) {
                    res.append(value).append(",");
                }
            }
        }
        if(res.toString().equals("")){
            return null;
        }
        res.deleteCharAt(res.lastIndexOf(","));
        return new Text(res.toString());
    }

}
