package com.yangyh.mr.fof;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @description:
 * @author: yangyh
 * @create: 2019-11-05 14:02
 */
public class MyKey implements WritableComparable<MyKey> {
    private String name;
    private String toFriend;
    private Integer coFriendNum;

    public MyKey() {
    }

    public MyKey(String name, String toFriend, Integer coFriendNum) {
        /** 自己的名字**/
        this.name = name;
        /** 对方的名字**/
        this.toFriend = toFriend;
        /** 共同好友数**/
        this.coFriendNum = coFriendNum;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getToFriend() {
        return toFriend;
    }

    public void setToFriend(String toFriend) {
        this.toFriend = toFriend;
    }

    public Integer getCoFriendNum() {
        return coFriendNum;
    }

    public void setCoFriendNum(Integer coFriendNum) {
        this.coFriendNum = coFriendNum;
    }

    @Override
    public int compareTo(MyKey o) {
        int result = this.name.compareTo(o.name);
        // name相同，表示给同一个人推荐的好友
        if (result == 0) {
            // 倒序
            result = o.coFriendNum.compareTo(this.coFriendNum);
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(toFriend);
        out.writeInt(coFriendNum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        setName(in.readUTF());
        setToFriend(in.readUTF());
        setCoFriendNum(in.readInt());
    }

    @Override
    public String toString() {
        return name + "_" + toFriend + "\t" + coFriendNum;
    }

}
