package com.bgt.mock.callRecord.bean;



/**
 * 联系人
 */
public class Contact{
    private String tel;
    private String name;

    public Contact(String name,String tel) {
        this.tel = tel;
        this.name = name;
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    public String toString(){
        return "Contact["+tel+", "+name+"]";
    }
}
