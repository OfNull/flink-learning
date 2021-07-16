package flinklearning._09Thread.producerconsumermode;

import java.io.UnsupportedEncodingException;

public class StringResever {
    public static String reseve(String str) {
        char[] chars = str.toCharArray();
        int length = chars.length;
        char[] newChars = new char[length];
        int n = 0;
        for (int i = length - 1; i >= 0; i--) {
            newChars[n] = chars[i];
            n++;
        }

        String s = new String(newChars);
        return s;
    }

    public static String reseve2(String str) throws UnsupportedEncodingException {

        byte[] bytes = str.getBytes();
        int length = bytes.length;
        int l = 0;
        int h = length - 1;
        byte tmp;
        for (; l != h && l< h; l++, h--) {
            tmp = bytes[l];
            bytes[l] = bytes[h];
            bytes[h] = tmp;
        }

        String s = new String(bytes, "utf-8");
        return s;
    }

    public static void main(String[] args) throws UnsupportedEncodingException {
        String reseve = reseve("我爱中国");
        System.out.println();
    }
}
