package Utils;

import java.util.Arrays;

public class BitMap {
    //保存数据的
    public byte[] bits;

    public Long counts = 0L;

    public BitMap(int capacity) {
        //1bit能存储8个数据，那么capacity数据需要多少个bit呢，capacity/8+1,右移3位相当于除以8
        this.bits = new byte[getIndex(capacity) + 1];
    }

    public void add(int num) {
        // num/8得到byte[]的index
        int arrayIndex = getIndex(num);

        // num%8得到在byte[index]的位置
        int position = getBitIndex(num);

        //将1左移position后，那个位置自然就是1，然后和以前的数据做|，这样，那个位置就替换成1了。
        bits[arrayIndex] |= 1 << position;

        this.counts += 1L;
    }

    public boolean contain(int num) {
        // num/8得到byte[]的index
        int arrayIndex = getIndex(num);

        // num%8得到在byte[index]的位置
        int position = getBitIndex(num);

        //将1左移position后，那个位置自然就是1，然后和以前的数据做&，判断是否为0即可
        return (bits[arrayIndex] & (1 << position)) != 0;
    }

    public void clear(int num) {
        // num/8得到byte[]的index
        int arrayIndex = getIndex(num);

        // num%8得到在byte[index]的位置
        int position = getBitIndex(num);

        //将1左移position后，那个位置自然就是1，然后对取反，再与当前值做&，即可清除当前的位置了.
        bits[arrayIndex] &= ~(1 << position);
    }

    private int getIndex(int num) {
        return (int) Math.floor((float) num / 8);
    }

    private int getBitIndex(int num)
    {
        return num % 8;
    }

    public static void main(String[] args) {
        BitMap bitmap = new BitMap(100);

        for (int i = 0; i < 60; i++) {
            bitmap.add(i);
        }

        System.out.println(Arrays.toString(bitmap.bits));

        for (int i = 0; i < 100; i++) {
            boolean isexsit = bitmap.contain(i);
            System.out.println(i + "是否存在:" + isexsit);
        }
    }
}

