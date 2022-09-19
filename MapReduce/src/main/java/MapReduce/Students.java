package MapReduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @BelongsProject: mapReduce
 * @BelongsPackage: mapReduce
 * @Author: FUJIWARA_ROOKIE
 * @Date: 2022/9/19 10:21
 */
public class Students implements WritableComparable<Students> {
    private long id;
    private String name;
    private int age;
    private String sex;
    private String className;

    public int compareTo(Students students) {
        return (int) (students.id - this.id);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id);
        dataOutput.writeUTF(name);
        dataOutput.writeInt(age);
        dataOutput.writeUTF(sex);
        dataOutput.writeUTF(className);
    }

    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readLong();
        name = dataInput.readUTF();
        age = dataInput.readInt();
        sex = dataInput.readUTF();
        className = dataInput.readUTF();
    }

    public void set(long id, String name, int age, String sex, String className) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.sex = sex;
        this.className = className;
    }

    @Override
    public String toString() {
        return id + "," + name + "," + age + "," + sex + "," + className;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String claaaName) {
        this.className = claaaName;
    }
}
