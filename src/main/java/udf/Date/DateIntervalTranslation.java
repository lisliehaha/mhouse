package udf.Date;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author ：liusengen
 * @date ：Created in 2019/9/24 5:49 下午
 * @description：
 * @modified By：
 * @version: $
 */
public class DateIntervalTranslation extends GenericUDF {
    private StringObjectInspector startInspector;
    private StringObjectInspector endInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
       if(args.length==2){
           startInspector=(StringObjectInspector) args[0];
           endInspector=(StringObjectInspector) args[1];
       }else{
           throw new UDFArgumentException("Usage : Erro Argument Num");
       }
        ObjectInspector returnOi= PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
        return ObjectInspectorFactory.getStandardListObjectInspector(returnOi);
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        List<String> dates = new ArrayList<String>();
        String startDate = startInspector.getPrimitiveJavaObject(args[0].get());
        String endDate = endInspector.getPrimitiveJavaObject(args[1].get());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date sdate;
        Date edate;
        Calendar cal = new Calendar.Builder().build();

        try {
            sdate = sdf.parse(startDate);
            edate = sdf.parse(endDate);
            dates.add(sdf.format(sdate));

            while (sdate.getTime() < edate.getTime()) {
                cal.setTime(sdate);
                cal.add(Calendar.DAY_OF_MONTH, 1);
                sdate = cal.getTime();
                dates.add(sdf.format(sdate));
            }
        } catch (Exception e) {
           return null;
        }
        return dates;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
