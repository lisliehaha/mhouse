package udf.regex;


import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author ：liusengen
 * @date ：Created in 2019/9/25 5:38 下午
 * @description：
 * @modified By：
 * @version: $
 */
public class TextToTime extends UDF {

    private String process1RegStr = "((\\d+)?\\.?\\d+)-((\\d+)?\\.?\\d+)(小时|天|日|分钟|个半小时|个小时|天半|周)";
    private String process2RegStr = "((\\d+)?\\.?\\d+)(小时|天|日|分钟|个半小时|个小时)-((\\d+)?\\.?\\d+)(小时|天|日|分钟|个半小时|个小时|天半|周)";
    private String process3RegStr = "((\\d+)?\\.?\\d+)(小时|天|日|分钟|个半小时|个小时|天半|周)";
    private String process4RegStr = "(半|一|二|三|四|五|六|七|八|九|十|十一|十二|二十|三十|四十|五十|六十|二十五|三十五|四十五|五十五)(小时|天|日|分钟|个半小时|个小时|天半|周)";


    public Double evaluate (String s){
        if(s==null||s.equals("")){
            return null;
        }
        double d;
        try {
            if( process(s)==0.0){
                return null;
            }
            return d = process(s);
        }catch (Exception e){
            return null;
        }
    }

    //数据预处理  //小时|小時|hours|hour|h', '小时'
    public String dataFormat(String s){
        String s1 = s.replace(" ", "")
                .replace("---", "-")
                .replace("——", "-")
                .replace("—","-")
                .replace("~", "-")
                .replace("～", "-")
                .replace("到", "-")
                .replace("至", "-")
                .replace("两","二")
                .replace("全","一")
                .replaceAll("小時|hours|hour|h", "小时")
                .replaceAll("Mins|Min|min|mins", "分钟");
        return s1;
    }

    //将中文数字转成阿拉伯数字
    public String chineseToint (String s){
        String [] ss = new String[100];
        for (int i=0;i<100;i++){
            String s1=ChineseNumToArabicNumUtil.arabicNumToChineseNum(i);
            if(s.contains(s1)) {
                s = s.replace(s1, "1");
            }
        }

        System.out.println(s);
        return s;

    }


    //加工总程序
    public double process (String s){
        s=dataFormat(s);
       // s=chineseToint(s);
        if(ismatch(process1RegStr,s)){
            return process1(s);
        }else if(ismatch(process2RegStr,s)){
            return process2(s);
        }else if(ismatch(process3RegStr,s)){
            return process3(s);
        }else if(ismatch(process4RegStr,s)){
            return process4(s);
        }
        return 0.0;
    }

     //流程1 ： 我们玩了3.5-4小时又玩了
     public  double  process1 (String s){
        String start_time= getPatternGroup(process1RegStr,s,1);
        String end_time= getPatternGroup(process1RegStr,s,3);
        String time_type= getPatternGroup(process1RegStr,s,5);
        double p = 0.0;
         double st = Double.parseDouble(start_time);
         double et = Double.parseDouble(end_time);
        if(time_type.equals("个半小时")){
            st += 0.5;
            p=1.0;
        }

        double diff_time = (et+st)/2;

        if(time_type.equals("小时")||time_type.equals("个小时")) p = 1;
        if(time_type.equals("分钟")) p =(double) 1/60;
        if(time_type.equals("天")) p = 8;
        if(time_type.equals("日")) p = 8;
        if(time_type.equals("天半")) p = 12;
        if(time_type.equals("周")) p = 56;

        if(et<st){
            return 0.0;
        }
        return p*diff_time;
    }

    //流程二 ：
    public  double  process2 (String s){

            String start_time= getPatternGroup(process2RegStr,s,1);
            String time_type1= getPatternGroup(process2RegStr,s,3);
            String end_time= getPatternGroup(process2RegStr,s,4);
            String time_type2= getPatternGroup(process2RegStr,s,6);
         double st = Double.parseDouble(start_time);
         double et = Double.parseDouble(end_time);

            double p = 0.0;
            double p2 = 0.0;
          if(time_type1.equals("个半小时")){
              st += 0.5;
            p=1.0;
          }
         if(time_type2.equals("个半小时")){
             et += 0.5;
             p2=1.0;
         }
            if(time_type1.equals("小时")||time_type1.equals("个小时")) p = 1;
            if(time_type1.equals("分钟")) p = (double) 1/60;
            if(time_type1.equals("天")) p = 8;
             if(time_type1.equals("日")) p = 8;
        if(time_type1.equals("周")) p = 56;
          if(time_type1.equals("天半")) p = 12;
            if(time_type2.equals("小时")||time_type2.equals("个小时")) p2 = 1;
            if(time_type2.equals("分钟")) p2 =(double) 1/60;
            if(time_type2.equals("天")) p2 = 8;
            if(time_type2.equals("日")) p = 8;
            if(time_type2.equals("天半")) p = 12;
        if(time_type2.equals("周")) p = 56;
            if(et*p2<st*p){
                return 0.0;
            }
            return (p2*et+p*st)/2;
    }

    //流程三
    public  double  process3 (String s){

        String start_time= getPatternGroup(process3RegStr,s,1);
        String time_type1= getPatternGroup(process3RegStr,s,3);
        double p = 0.0;
        double st = Double.parseDouble(start_time);
        if(time_type1.equals("个半小时")){
            st += 0.5;
            p=1.0;
        }
        if(time_type1.equals("小时")||time_type1.equals("个小时")) p = 1;
        if(time_type1.equals("分钟")) p = (double) 1/60;
        if(time_type1.equals("天")) p = 8;
        if(time_type1.equals("日")) p = 8;
        if(time_type1.equals("天半")) p = 12;
        if(time_type1.equals("周")) p = 56;
        return p*st;
    }


    //流程四
    public  double  process4 (String s){

        String start_time= getPatternGroup(process4RegStr,s,1);
        String time_type1= getPatternGroup(process4RegStr,s,2);
        double st=0.0;
        double p = 0.0;
        if(start_time.equals("半")){
            st=0.5;
        }else{
           st= (double) ChineseNumToArabicNumUtil.chineseNumToArabicNum(start_time);
        }
        if(time_type1.equals("个半小时")){
            st += 0.5;
            p=1.0;
        }
        if(time_type1.equals("小时")||time_type1.equals("个小时")) p = 1;
        if(time_type1.equals("分钟")) p = (double) 1/60;
        if(time_type1.equals("天")) p = 8;
        if(time_type1.equals("日")) p = 8;
        if(time_type1.equals("天半")) p = 12;
        if(time_type1.equals("周")) p = 56;

        return p*st;
    }


    public  boolean ismatch (String pattern,String content){
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(content);
        return m.find();
    }

    public String getPatternGroup(String pattern,String content,int index){
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(content);
        if(m.find()){
            return m.group(index);
        }
        return null;
    }

    //

    public static void main(String[] args) {
        String line="我们玩两天";
        TextToTime textToTime = new TextToTime();
        Pattern p = Pattern.compile("((\\d+)?\\.?\\d+)(小时|天|日|分钟)");
        System.out.println(textToTime.process(line));
        //1346
        Pattern p2 = Pattern.compile("((\\d+)?\\.?\\d+)(小时|天|日|分钟)");
        Matcher m = p2.matcher(line);
        if (m.find( )) {
            //13
            System.out.println(m.group(0));
            System.out.println(m.group(1));
            System.out.println(m.group(2));
            System.out.println(m.group(3));

        }




    }
}
