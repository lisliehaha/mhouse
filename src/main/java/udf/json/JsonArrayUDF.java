package udf.json;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
/**
 * @author ：liusengen
 * @date ：Created in 2019-08-16 10:42
 * @description： 输入json的array字符串，返回array类型
 * @modified By：
 * @version: v1.0$
 */

@Description(name = "json_split",
        value = "_FUNC_(json) - Returns a array of JSON strings from a JSON Array"
)
public  class JsonArrayUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(JsonArrayUDF.class);
    private StringObjectInspector stringInspector;
    private InspectorHandle inspHandle;


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        try {
            String jsonString = this.stringInspector.getPrimitiveJavaObject(arguments[0].get());

            //// Logic is the same as "from_json"
            ObjectMapper om = new ObjectMapper();
            JsonNode jsonNode = om.readTree(jsonString);
            return inspHandle.parseJson(jsonNode);


        } catch (JsonProcessingException jsonProc) {
            LOG.error("JsonProcessingException ");
            return null;
        } catch (IOException e) {
            LOG.error("IOException ");
            return null;
        } catch (NullPointerException npe) {
            LOG.error("NullPointerException");
            return null;
        }

    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "json_split(" + arg0[0] + ")";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        if (arguments.length != 1 && arguments.length != 2) {
            throw new UDFArgumentException("Usage : json_split( jsonstring, optional typestring) ");
        }
        if (!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)
                || ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("Usage : json_split( jsonstring, optional typestring) ");
        }
        stringInspector = (StringObjectInspector) arguments[0];

        if (arguments.length > 1) {
            if (!arguments[1].getCategory().equals(ObjectInspector.Category.PRIMITIVE)
                    || ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
                throw new UDFArgumentException("Usage : json_split( jsonstring, optional typestring) ");
            }
            if (!(arguments[1] instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("Usage : json_split( jsonstring, typestring) : typestring must be constant");
            }
            ConstantObjectInspector typeInsp = (ConstantObjectInspector) arguments[1];
            String typeString = ((Text) typeInsp.getWritableConstantValue()).toString();
            TypeInfo valType = TypeInfoUtils.getTypeInfoFromTypeString(typeString);

            ObjectInspector valInsp = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(valType);
            ObjectInspector setInspector = ObjectInspectorFactory.getStandardListObjectInspector(valInsp);
            inspHandle = InspectorHandle.InspectorHandleFactory.GenerateInspectorHandle(setInspector);
            return inspHandle.getReturnType();

        } else {
            ObjectInspector valInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

            ObjectInspector setInspector = ObjectInspectorFactory.getStandardListObjectInspector(valInspector);
            inspHandle = InspectorHandle.InspectorHandleFactory.GenerateInspectorHandle(setInspector);
            return inspHandle.getReturnType();
        }
    }

}


