package com.bgt.mock.callRecord;

import com.bgt.mock.callRecord.bean.Calllog;
import com.bgt.mock.callRecord.bean.Contact;

import java.io.*;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

/**
 *杨慧慧，尹晓蓉，尹相予，韩月梅，刘洪涛，武彩艳
 */
public class produce {
    private static BufferedReader reader=null;
    private static PrintWriter writer=null;
    private static String  line=null;
    private static ArrayList<Contact> ts = new ArrayList<Contact>();
    public static void main(String[] args) {
        readFile("dataInput/contact.log");
        produce();
    }

    public static void readFile(String path) {

        try {
            reader=new BufferedReader(new InputStreamReader(new FileInputStream(path),"utf-8"));//字符流和字节流不可以直接转换，需要转换流InputStreamReader
            while((line=reader.readLine())!=null){
                String[] split = line.split("\t");
                Contact contact = new Contact(split[0], split[1]);
                ts.add(contact);
            }
        }catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void produce(){
        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream("dataInput/callRecord.log"), "UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {
            int call1Index = new Random().nextInt(ts.size());
            int call2Index;
            while (true) {
                call2Index = new Random().nextInt(ts.size());
                if (call1Index == call2Index) {
                    break;
                }

                Contact call1 = ts.get(call1Index);
                Contact call2 = ts.get(call2Index);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
                long startTime = 0;
                long endTime = 0;
                try {
                    startTime = sdf.parse("20190101000000").getTime();
                    endTime = sdf.parse("20200101000000").getTime();

                } catch (ParseException e) {
                    e.printStackTrace();
                }
                //通话时间(时间戳)
                long calltime = startTime + (long) ((endTime - startTime) * Math.random());
                String callTimeString = sdf.format(new Date(calltime));
                //通话间隔
                String duration = format(new Random().nextInt(3000), 4);

                //生成通话记录
                Calllog log = new Calllog(call1.getTel(), call2.getTel(), callTimeString, duration);
                try {
                    writer.println(log.toString());
                    writer.flush();
                    System.out.println(log.toString());
                    Thread.sleep(500);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
    //指定通话时间的格式
    public static String format(int num,int length){
        StringBuffer stringBuffer = new StringBuffer();
        for(int i=1;i<=length;i++){
            stringBuffer.append("0");
        }
        DecimalFormat df = new DecimalFormat(stringBuffer.toString());//这里到时候传值就ok
        return df.format(num);
    }
}
