package Hadoop;

import org.apache.hadoop.io.Text;

import java.io.UnsupportedEncodingException;

public class StringComparisonTest {
    public static void main(String[] args) throws UnsupportedEncodingException {
        String s= "big";
        System.out.println("String:"+s);
        System.out.println(s.length());
        System.out.println(s.getBytes("UTF-8").length);
        System.out.println(s.indexOf("b"));
        System.out.println(s.indexOf("i"));
        System.out.println(s.indexOf("g"));
        System.out.println(s.charAt(0));
        System.out.println(s.charAt(1));
        System.out.println(s.charAt(2));
        System.out.println("********************");
        Text t=new Text("big");
        System.out.println("Text:"+t);
        System.out.println(t.getLength());
        System.out.println(t.find("b"));
        System.out.println(t.find("i"));
        System.out.println(t.find("g"));
        System.out.println(t.charAt(0));
        System.out.println((int)'b');

    }


}
